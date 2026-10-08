import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../Migrations.ts";

const indexes = [
  "orchestration_v2_heads_up_identity_idx",
  "orchestration_v2_projection_turn_items_user_message_idx",
  "orchestration_v2_projection_nodes_live_idx",
];
const typed = JSON.stringify({
  message: "unchanged",
  extra: { retained: true },
  headsUp: {
    noteId: "typed",
    tag: "Heads up",
    line: "Typed",
    sourceThreadId: "source",
  },
});
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_v2_projection_turn_items
    (turn_item_id, thread_id, ordinal, type, status, updated_at, payload_json)
    VALUES ('typed', 'thread', 0, 'system_notice', 'completed', 'now', ${typed})`;
  for (let i = 0; i < 503; i++) {
    yield* sql`INSERT INTO orchestration_v2_projection_turn_items
      (turn_item_id, thread_id, ordinal, type, status, updated_at, payload_json)
      VALUES (${`legacy-${String(i).padStart(4, "0")}`}, 'thread', ${i + 1},
        'system_notice', 'completed', 'now', ${JSON.stringify({ message: `[ysk:n-${i}] Heads up · Cache (cache.ts:12)\n\nExplanation`, extra: i })})`;
  }
});
const verify = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  assert.deepEqual(
    yield* sql`SELECT payload_json FROM orchestration_v2_projection_turn_items WHERE turn_item_id = 'typed'`,
    [{ payload_json: typed }],
  );
  const rows = yield* sql<{
    payload_json: string;
  }>`SELECT payload_json FROM orchestration_v2_projection_turn_items WHERE turn_item_id LIKE 'legacy-%' ORDER BY turn_item_id`;
  assert.equal(rows.length, 503);
  rows.forEach((row, i) =>
    assert.deepEqual(JSON.parse(row.payload_json), {
      message: `[ysk:n-${i}] Heads up · Cache (cache.ts:12)\n\nExplanation`,
      extra: i,
      headsUp: {
        noteId: `n-${i}`,
        tag: "Heads up",
        line: "Cache",
        evidence: "cache.ts:12",
        explanation: "Explanation",
      },
    }),
  );
  const names = yield* sql<{ name: string }>`SELECT name FROM sqlite_master WHERE type = 'index'`;
  for (const name of indexes)
    assert.ok(
      names.some((row) => row.name === name),
      name,
    );
  assert.deepEqual(yield* runMigrations(), []);
});

describe("61 fork migration convergence", () => {
  it.effect("migrates a fresh database and repeats without work", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const applied = yield* runMigrations();
      assert.deepEqual(applied.slice(-3), [
        [59, "McpAppModelContext"],
        [60, "ThreadSnapshotWindowIndexes"],
        [61, "HeadsUpInbox"],
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM mcp_app_model_context`, []);
      assert.deepEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect.each(["upstream60", "fork60", "oldfork59", "unknown60"] as const)(
    "preserves data and converges %s",
    (history) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: history === "oldfork59" ? 58 : 60 });
        if (history === "oldfork59") {
          yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES (59, 'HeadsUpInbox')`;
        } else if (history !== "upstream60") {
          yield* sql`UPDATE effect_sql_migrations SET name = ${history === "fork60" ? "HeadsUpInbox" : "UnknownFork"} WHERE migration_id = 60`;
          for (const name of indexes.slice(1)) yield* sql`DROP INDEX ${sql(name)}`;
        }
        if (history !== "oldfork59") {
          yield* sql`INSERT INTO mcp_app_model_context VALUES ('thread', 'item', 'server', 'tool', 'payload\nbytes', 'now')`;
        }
        yield* seed;
        assert.deepEqual(
          yield* runMigrations(),
          history === "oldfork59"
            ? [
                [60, "ThreadSnapshotWindowIndexes"],
                [61, "HeadsUpInbox"],
              ]
            : [[61, "HeadsUpInbox"]],
        );
        assert.deepEqual(
          yield* sql`SELECT migration_id, name FROM effect_sql_migrations WHERE migration_id >= 59 ORDER BY migration_id`,
          [
            { migration_id: 59, name: "McpAppModelContext" },
            {
              migration_id: 60,
              name: history === "unknown60" ? "UnknownFork" : "ThreadSnapshotWindowIndexes",
            },
            { migration_id: 61, name: "HeadsUpInbox" },
          ],
        );
        assert.deepEqual(
          yield* sql`SELECT * FROM mcp_app_model_context`,
          history === "oldfork59"
            ? []
            : [
                {
                  thread_id: "thread",
                  item_id: "item",
                  server: "server",
                  tool: "tool",
                  text: "payload\nbytes",
                  updated_at: "now",
                },
              ],
        );
        yield* verify;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect(
    "rolls back schema, payloads and ledger together when ledger rename aborts, then retries",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 58 });
        yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES (59, 'HeadsUpInbox'), (60, 'HeadsUpInbox')`;
        yield* seed;
        const before =
          yield* sql`SELECT * FROM orchestration_v2_projection_turn_items ORDER BY turn_item_id`;
        const ledger = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
        const schema = yield* sql`SELECT * FROM sqlite_master ORDER BY name`;
        yield* sql`CREATE TRIGGER abort_rename BEFORE UPDATE OF name ON effect_sql_migrations WHEN OLD.migration_id = 60 BEGIN SELECT RAISE(ABORT, 'rename blocked'); END`;
        assert.ok(Exit.isFailure(yield* Effect.exit(runMigrations())));
        assert.deepEqual(
          yield* sql`SELECT * FROM orchestration_v2_projection_turn_items ORDER BY turn_item_id`,
          before,
        );
        assert.deepEqual(
          yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
          ledger,
        );
        yield* sql`DROP TRIGGER abort_rename`;
        assert.deepEqual(yield* sql`SELECT * FROM sqlite_master ORDER BY name`, schema);
        assert.deepEqual(yield* runMigrations(), [[61, "HeadsUpInbox"]]);
        yield* verify;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});
