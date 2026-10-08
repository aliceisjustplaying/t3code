import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ApplicationEvents from "../persistence/OrchestrationEventStore.ts";
import * as Projections from "./ProjectionStore.ts";
import * as Inbox from "./HeadsUpInbox.ts";
import * as EventStore from "./EventStore.ts";
import * as EventSink from "./EventSink.ts";
import Migration from "../persistence/Migrations/060_HeadsUpInbox.ts";

const database = Sqlite.layerMemory;
const events = ApplicationEvents.layer.pipe(Layer.provide(database));
const projectionsLayer = Projections.layer.pipe(Layer.provide(database));
const layer = Layer.mergeAll(
  database,
  projectionsLayer,
  Inbox.layer.pipe(Layer.provide(Layer.merge(database, events))),
);
const streamingLayer = Layer.merge(
  layer,
  EventSink.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        database,
        projectionsLayer,
        EventStore.layerFromOrchestrationEventStore.pipe(Layer.provide(events)),
      ),
    ),
  ),
);
const at = (minute: number) => DateTime.makeUnsafe(Date.UTC(2026, 9, 6, 0, minute));
const source = ThreadId.make("child");
const parent = ThreadId.make("parent");
const session = ProviderThreadId.make("session-1");
const note = (
  id: string,
  minute: number,
  overrides: {
    readonly threadId?: ThreadId;
    readonly forwarded?: boolean;
    readonly providerThreadId?: ProviderThreadId;
  } = {},
): OrchestrationV2DomainEvent => ({
  id: EventId.make(`event:${id}`),
  threadId: overrides.threadId ?? source,
  type: "turn-item.updated",
  occurredAt: at(minute),
  payload: {
    id: TurnItemId.make(id),
    threadId: overrides.threadId ?? source,
    runId: null,
    nodeId: null,
    providerThreadId: overrides.providerThreadId ?? session,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: minute,
    type: "system_notice",
    status: "completed",
    title: "Title",
    message: `[ysk:n-1] Heads up · Shared cache (cache.ts:12)\n\nA cache explanation.`,
    startedAt: at(minute),
    completedAt: at(minute),
    updatedAt: at(minute),
    ...(overrides.forwarded
      ? {
          headsUp: {
            sourceThreadId: source,
            noteId: "n-1",
            tag: "Heads up",
            line: "Child: Shared cache",
            explanation: "A cache explanation.",
          },
        }
      : {}),
  },
});
const create = (
  threadId: ThreadId,
  archived = false,
): Extract<OrchestrationV2DomainEvent, { type: "thread.created" }> => ({
  id: EventId.make(`create:${threadId}`),
  threadId,
  type: "thread.created",
  occurredAt: at(0),
  payload: {
    id: threadId,
    projectId: ProjectId.make("project"),
    title: `Title ${threadId}`,
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: ProviderInstanceId.make("pi"),
    modelSelection: { instanceId: ProviderInstanceId.make("pi"), model: "default" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: at(0),
    updatedAt: at(0),
    archivedAt: archived ? at(1) : null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  },
});

// Query boundary owns dedup, full results and durable counts. A projection/window
// fixture cannot fake those SQL semantics or source-session isolation.
it.effect(
  "queries durable environment notices, deduplicates source copies and returns all entries without losing old or archived notes",
  () =>
    Effect.gen(function* () {
      const projections = yield* Projections.ProjectionStoreV2;
      const inbox = yield* Inbox.HeadsUpInbox;
      yield* projections.apply(create(source, true));
      yield* projections.apply(create(parent));
      yield* projections.apply(note("original", 1));
      yield* projections.apply(note("repeat", 4));
      yield* projections.apply(note("forwarded", 5, { threadId: parent, forwarded: true }));
      yield* projections.apply(
        note("new-session", 6, { providerThreadId: ProviderThreadId.make("session-2") }),
      );
      yield* projections.apply(note("independent-parent", 7, { threadId: parent }));
      const all = yield* inbox.list({ view: "unresolved" });
      assert.equal(all.unreadCount, 3);
      assert.equal(all.unresolvedCount, 3);
      assert.deepEqual(
        all.items.map((entry) => entry.turnItemId),
        ["independent-parent", "new-session", "original"],
      );
      const original = all.items[2];
      assert.equal(original?.sourceThreadId, source);
      assert.equal(original?.targetThreadId, parent);
      assert.equal(original?.sourceThreadTitle, "Title child");
      assert.equal(original?.note.line, "Shared cache");
      assert.equal(original?.note.evidence, "cache.ts:12");
      assert.equal(original?.note.explanation, "A cache explanation.");

      yield* projections.apply({
        id: EventId.make("read"),
        threadId: parent,
        type: "thread.heads-up.updated",
        occurredAt: at(8),
        payload: {
          sourceThreadId: source,
          providerThreadId: session,
          noteId: "n-1",
          readAt: DateTime.formatIso(at(8)),
        },
      });
      assert.equal((yield* inbox.list({ view: "unresolved" })).unreadCount, 2);
      assert.equal((yield* inbox.list({ view: "reviewed" })).reviewedCount, 0);
      yield* projections.apply({
        id: EventId.make("review"),
        threadId: source,
        type: "thread.heads-up.updated",
        occurredAt: at(9),
        payload: {
          sourceThreadId: source,
          providerThreadId: session,
          noteId: "n-1",
          resolution: "knew",
        },
      });
      // A late read must not overwrite Knew, and re-notification must inherit both.
      yield* projections.apply({
        id: EventId.make("late-read"),
        threadId: parent,
        type: "thread.heads-up.updated",
        occurredAt: at(10),
        payload: {
          sourceThreadId: source,
          providerThreadId: session,
          noteId: "n-1",
          readAt: DateTime.formatIso(at(10)),
        },
      });
      yield* projections.apply(note("reconnected", 11));
      const reviewed = yield* inbox.list({ view: "reviewed" });
      assert.equal(reviewed.items.length, 1);
      assert.equal(reviewed.items[0]?.note.resolution, "knew");
      assert.equal(reviewed.unresolvedCount, 2);
      yield* projections.apply({
        id: EventId.make("undo"),
        threadId: parent,
        type: "thread.heads-up.updated",
        occurredAt: at(12),
        payload: {
          sourceThreadId: source,
          providerThreadId: session,
          noteId: "n-1",
          resolution: null,
        },
      });
      const restored = yield* inbox.list({ view: "unresolved" });
      assert.equal(restored.items.length, 3);
      assert.equal(restored.unreadCount, 2);
      assert.equal((yield* inbox.list({ view: "reviewed" })).items.length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect("backfills untyped retained notices, not only recent chat history", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const projections = yield* Projections.ProjectionStoreV2;
    const inbox = yield* Inbox.HeadsUpInbox;
    yield* projections.apply(create(source));
    yield* projections.apply(note("legacy", 1));
    yield* sql`UPDATE orchestration_v2_projection_turn_items SET payload_json = json_remove(payload_json, '$.headsUp')`;
    yield* sql`DROP INDEX orchestration_v2_heads_up_identity_idx`;
    assert.equal((yield* inbox.list({ view: "unresolved" })).items.length, 0);
    yield* Migration;
    const page = yield* inbox.list({ view: "unresolved" });
    assert.equal(page.items.length, 1);
    assert.equal(page.items[0]?.note.evidence, "cache.ts:12");
  }).pipe(Effect.provide(layer)),
);

// The live summary owns a different risk from the SQL query: a notice committed
// between its initial snapshot and live attachment must replay, including old
// untyped notices. Reconnecting another device must read the persisted answer.
it.live(
  "replays arrivals across subscription attachment and reconnects with durable reviewed counts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const inbox = yield* Inbox.HeadsUpInbox;
        yield* sink.write({ events: [create(source)] });
        const snapshotted = yield* Deferred.make<void>();
        const attach = yield* Deferred.make<void>();
        let initial = true;
        const watcher = yield* inbox.subscribe.pipe(
          Stream.tap(() => {
            if (!initial) return Effect.void;
            initial = false;
            return Deferred.succeed(snapshotted, undefined).pipe(
              Effect.andThen(Deferred.await(attach)),
            );
          }),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* Deferred.await(snapshotted);
        // Force the race, rather than hoping a scheduler happens to expose it.
        yield* sink.write({ events: [note("arrived-before-attach", 1)] });
        yield* Deferred.succeed(attach, undefined);
        const received = yield* Fiber.join(watcher);
        assert.equal(received[0]?.unreadCount, 0);
        assert.equal(received[1]?.unreadCount, 1);
        assert.equal(received[1]?.unresolvedCount, 1);
        yield* sink.write({
          events: [
            {
              id: EventId.make("review-after-attach"),
              threadId: source,
              type: "thread.heads-up.updated",
              occurredAt: at(2),
              payload: {
                sourceThreadId: source,
                providerThreadId: session,
                noteId: "n-1",
                resolution: "dismiss",
                readAt: DateTime.formatIso(at(2)),
              },
            },
          ],
        });
        const reconnect = yield* inbox.subscribe.pipe(Stream.take(1), Stream.runCollect);
        assert.equal(reconnect[0]?.unreadCount, 0);
        assert.equal(reconnect[0]?.unresolvedCount, 0);
        assert.equal(reconnect[0]?.reviewedCount, 1);
        assert.equal(
          (yield* inbox.list({ view: "reviewed" })).items[0]?.note.resolution,
          "dismiss",
        );
      }),
    ).pipe(Effect.provide(streamingLayer), Effect.timeout("5 seconds")),
);

it.effect("returns all 75 notices in project recency order, not notice recency", () =>
  Effect.gen(function* () {
    const projections = yield* Projections.ProjectionStoreV2;
    const inbox = yield* Inbox.HeadsUpInbox;
    const sql = yield* SqlClient.SqlClient;
    const old = ThreadId.make("old-project");
    const recent = ThreadId.make("recent-project");
    const archived = ThreadId.make("archived-project");
    for (const id of [old, recent, archived]) {
      const event = create(id, id === archived);
      yield* projections.apply({
        ...event,
        payload: { ...event.payload, projectId: ProjectId.make(id === archived ? old : id) },
      });
    }
    // Notice updates make the old project's thread.updatedAt newer. The latest
    // user message must still win, matching the landing project sort source.
    for (let index = 0; index < 75; index++) {
      yield* projections.apply(
        note(`global-${index}`, index + 1, {
          threadId: index < 35 ? recent : old,
          providerThreadId: ProviderThreadId.make(`global-session-${index}`),
        }),
      );
    }
    yield* sql`INSERT INTO orchestration_v2_projection_messages
      (message_id, thread_id, role, streaming, created_at, updated_at, payload_json)
      VALUES ('old-user', ${old}, 'user', 0, '2026-10-06T00:01:00.000Z', '2026-10-06T00:01:00.000Z', '{}'),
        ('recent-user', ${recent}, 'user', 0, '2026-10-06T00:10:00.000Z', '2026-10-06T00:10:00.000Z', '{}'),
        ('archived-user', ${archived}, 'user', 0, '2026-10-06T23:00:00.000Z', '2026-10-06T23:00:00.000Z', '{}')`;
    const all = yield* inbox.list({ view: "unresolved" });
    assert.equal(all.unreadCount, 75);
    assert.equal(all.items.length, 75);
    assert.equal(all.nextCursor, null);
    assert.deepEqual(
      all.items.map((entry) => entry.projectId),
      [...Array(35).fill(recent), ...Array(40).fill(old)],
    );
    assert.equal(new Set(all.items.map((entry) => entry.id)).size, 75);
  }).pipe(Effect.provide(layer)),
);
