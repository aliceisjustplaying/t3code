import { getThreadSortTimestamp, toSortableTimestamp } from "@t3tools/shared/threadSortTimestamp";
import {
  HeadsUpInboxEntry,
  HeadsUpInboxError,
  HeadsUpInboxSummary,
  type HeadsUpInboxInput,
  type HeadsUpInboxPage,
  OrchestrationV2HeadsUp,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import { parseHeadsUpNotice } from "./HeadsUpNotice.ts";

const encodeProjectIds = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
const Row = HeadsUpInboxEntry.mapFields((fields) => ({
  ...fields,
  note: Schema.fromJsonString(OrchestrationV2HeadsUp),
}));

export class HeadsUpInbox extends Context.Service<
  HeadsUpInbox,
  {
    readonly list: (input: HeadsUpInboxInput) => Effect.Effect<HeadsUpInboxPage, HeadsUpInboxError>;
    readonly subscribe: Stream.Stream<HeadsUpInboxSummary, HeadsUpInboxError>;
  }
>()("t3/orchestration-v2/HeadsUpInbox") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* OrchestrationEventStore.OrchestrationEventStore;
  // Read the durable notice projection directly, never a bounded chat snapshot.
  // Source session + source thread + note id distinguish independent notes,
  // while the child row, repeat notifications and forwarded parent row combine.
  const candidates = sql`
    WITH notes AS (
      SELECT items.*,
        COALESCE(json_extract(items.payload_json, '$.headsUp.sourceThreadId'), items.thread_id) AS source_id,
        json_array(COALESCE(json_extract(items.payload_json, '$.headsUp.sourceThreadId'), items.thread_id),
          items.provider_thread_id, json_extract(items.payload_json, '$.headsUp.noteId')) AS identity,
        COALESCE(json_extract(items.payload_json, '$.startedAt'), json_extract(items.payload_json, '$.completedAt'), items.updated_at) AS created_at,
        json_extract(items.payload_json, '$.headsUp.readAt') AS read_at,
        json_extract(items.payload_json, '$.headsUp.resolution') AS resolution
      FROM orchestration_v2_projection_turn_items AS items
      INNER JOIN orchestration_v2_projection_threads AS threads ON threads.thread_id = items.thread_id
      WHERE items.type = 'system_notice' AND json_extract(items.payload_json, '$.headsUp.noteId') IS NOT NULL
        AND threads.deleted_at IS NULL
    ), grouped AS (
      SELECT identity, MIN(created_at) AS created_at, MAX(read_at) AS read_at, MAX(resolution) AS resolution,
        MIN(CASE WHEN thread_id <> source_id THEN thread_id END) AS parent_id
      FROM notes GROUP BY identity
    ), ranked AS (
      SELECT notes.*, ROW_NUMBER() OVER (PARTITION BY identity ORDER BY
        CASE WHEN thread_id = source_id THEN 0 ELSE 1 END, ordinal, turn_item_id) AS rank
      FROM notes
    )
  `;
  const summary = Effect.fn("HeadsUpInbox.summary")(
    function* () {
      const counts = yield* sql`
      ${candidates}
      SELECT COUNT(CASE WHEN resolution IS NULL AND read_at IS NULL THEN 1 END) AS "unreadCount",
        COUNT(CASE WHEN resolution IS NULL THEN 1 END) AS "unresolvedCount",
        COUNT(CASE WHEN resolution IS NOT NULL THEN 1 END) AS "reviewedCount"
      FROM grouped
    `;
      return yield* Schema.decodeUnknownEffect(HeadsUpInboxSummary)({
        ...counts[0],
        sequence: yield* events.latestApplicationSequence,
      });
    },
    sql.withTransaction,
    Effect.mapError((cause) => new HeadsUpInboxError({ operation: "query", cause })),
  );

  const list: HeadsUpInbox["Service"]["list"] = Effect.fn("HeadsUpInbox.list")(function* (input) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const state = yield* summary();
          const projectRows = yield* sql<{
            projectId: string;
            title: string;
            createdAt: string | null;
            updatedAt: string | null;
          }>`
            SELECT ids.project_id AS "projectId", COALESCE(p.title, ids.project_id) AS title,
              p.created_at AS "createdAt", p.updated_at AS "updatedAt"
            FROM (SELECT project_id FROM projection_projects WHERE deleted_at IS NULL
              UNION SELECT project_id FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL) ids
            LEFT JOIN projection_projects p ON p.project_id = ids.project_id AND p.deleted_at IS NULL
          `;
          const threadRows = yield* sql<{
            projectId: string;
            createdAt: string;
            updatedAt: string;
            latestUserMessageAt: string | null;
          }>`
            SELECT t.project_id AS "projectId", t.created_at AS "createdAt", t.updated_at AS "updatedAt",
              (SELECT m.updated_at FROM orchestration_v2_projection_messages m
                WHERE m.thread_id = t.thread_id AND m.role = 'user'
                ORDER BY m.updated_at DESC, m.message_id DESC LIMIT 1) AS "latestUserMessageAt"
            FROM orchestration_v2_projection_threads t
            WHERE t.deleted_at IS NULL AND t.archived_at IS NULL
          `;
          // Reuse the landing/sidebar timestamp source, not notice activity.
          const activity = new Map<string, number>();
          for (const thread of threadRows) {
            activity.set(
              thread.projectId,
              Math.max(
                activity.get(thread.projectId) ?? Number.NEGATIVE_INFINITY,
                getThreadSortTimestamp(thread, "updated_at"),
              ),
            );
          }
          const projectTimestamp = (project: (typeof projectRows)[number]) =>
            activity.get(project.projectId) ??
            toSortableTimestamp(project.updatedAt ?? project.createdAt ?? undefined) ??
            Number.NEGATIVE_INFINITY;
          const projectOrder = [...projectRows].sort((left, right) => {
            const a = projectTimestamp(left);
            const b = projectTimestamp(right);
            return (
              (a === b ? 0 : b > a ? 1 : -1) ||
              left.title.localeCompare(right.title) ||
              left.projectId.localeCompare(right.projectId)
            );
          });
          const projectIds = encodeProjectIds(projectOrder.map((project) => project.projectId));
          const rows = yield* sql`
        ${candidates}, project_order AS (SELECT value AS project_id, CAST(key AS INTEGER) AS position FROM json_each(${projectIds}))
        SELECT target.project_id AS "projectId", COALESCE(project.title, target.project_id) AS "projectTitle", ranked.identity AS id, ranked.thread_id AS "threadId", ranked.turn_item_id AS "turnItemId",
          ranked.source_id AS "sourceThreadId", COALESCE(source.title, ranked.source_id) AS "sourceThreadTitle",
          COALESCE(grouped.parent_id, ranked.thread_id) AS "targetThreadId",
          COALESCE(target.title, ranked.source_id) AS "targetThreadTitle",
          ranked.provider_thread_id AS "providerThreadId", grouped.created_at AS "createdAt", grouped.read_at AS "readAt",
          json_remove(json_set(json_extract(ranked.payload_json, '$.headsUp'), '$.resolution', grouped.resolution),
            CASE WHEN grouped.resolution IS NULL THEN '$.resolution' ELSE '$.__absent' END) AS note
        FROM ranked INNER JOIN grouped ON grouped.identity = ranked.identity
        LEFT JOIN orchestration_v2_projection_threads AS source ON source.thread_id = ranked.source_id
        LEFT JOIN orchestration_v2_projection_threads AS target ON target.thread_id = COALESCE(grouped.parent_id, ranked.thread_id)
        INNER JOIN project_order ON project_order.project_id = target.project_id
        LEFT JOIN projection_projects project ON project.project_id = target.project_id AND project.deleted_at IS NULL
        WHERE ranked.rank = 1
          AND ${input.view === "unresolved" ? sql`grouped.resolution IS NULL` : sql`grouped.resolution IS NOT NULL`}
        ORDER BY project_order.position, grouped.created_at DESC, ranked.identity DESC
      `;
          const entries = yield* Schema.decodeUnknownEffect(Schema.Array(Row))(rows).pipe(
            Effect.mapError((cause) => new HeadsUpInboxError({ operation: "decode", cause })),
          );
          return { ...state, items: entries, nextCursor: null };
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          Schema.is(HeadsUpInboxError)(cause)
            ? cause
            : new HeadsUpInboxError({ operation: "query", cause }),
        ),
      );
  });

  const subscribe = Stream.unwrap(
    Effect.gen(function* () {
      const initial = yield* summary();
      // Snapshot + sequence replay closes the subscribe/read race. Project before
      // buffering so an environment inbox never retains all transcript payloads.
      const changes = events
        .streamProjectedApplicationEvents({
          afterSequence: initial.sequence,
          project: (stored) => ({
            sequence: stored.sequence,
            relevant:
              "aggregateKind" in stored
                ? stored.type === "project.deleted" || stored.type === "project.meta-updated"
                : stored.event.type === "thread.heads-up.updated" ||
                  stored.event.type === "thread.deleted" ||
                  stored.event.type === "thread.metadata-updated" ||
                  stored.event.type === "thread.archived" ||
                  stored.event.type === "thread.unarchived" ||
                  (stored.event.type === "message.updated" &&
                    stored.event.payload.role === "user") ||
                  (stored.event.type === "turn-item.updated" &&
                    stored.event.payload.type === "system_notice" &&
                    (stored.event.payload.headsUp !== undefined ||
                      parseHeadsUpNotice(stored.event.payload.message) !== undefined)),
          }),
        })
        .pipe(
          Stream.filter((event) => event.relevant),
          Stream.groupedWithin(100, "100 millis"),
          Stream.mapEffect(() => summary()),
        );
      return Stream.concat(Stream.succeed(initial), changes);
    }),
  ).pipe(
    Stream.mapError((cause) =>
      Schema.is(HeadsUpInboxError)(cause)
        ? cause
        : new HeadsUpInboxError({ operation: "subscribe", cause }),
    ),
  );
  return HeadsUpInbox.of({ list, subscribe });
});

export const layer = Layer.effect(HeadsUpInbox, make);
