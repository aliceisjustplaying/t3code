import { OrchestrationV2HeadsUp } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { parseHeadsUpNotice } from "../../orchestration-v2/HeadsUpNotice.ts";

import migrateMcpAppModelContext from "./059_McpAppModelContext.ts";
import migrateThreadSnapshotWindowIndexes from "./060_ThreadSnapshotWindowIndexes.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Fork builds used upstream IDs 59 and 60 for YSK. The migrator skips
  // recorded IDs, so apply both idempotent upstream changes before reconciling.
  yield* migrateMcpAppModelContext;
  yield* migrateThreadSnapshotWindowIndexes;
  // Older retained notices predate typed headsUp metadata. Backfill all of
  // them, not just the loaded/recent chat windows. Paging bounds migration RAM.
  let after = "";
  while (true) {
    const rows = yield* sql<{ turn_item_id: string; payload_json: string }>`
      SELECT turn_item_id, payload_json FROM orchestration_v2_projection_turn_items
      WHERE type = 'system_notice' AND turn_item_id > ${after}
        AND json_extract(payload_json, '$.headsUp.noteId') IS NULL
      ORDER BY turn_item_id LIMIT 500
    `;
    if (rows.length === 0) break;
    for (const row of rows) {
      const payload = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(Schema.Struct({ message: Schema.optional(Schema.String) })),
      )(row.payload_json);
      const note = payload.message === undefined ? undefined : parseHeadsUpNotice(payload.message);
      if (note !== undefined) {
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2HeadsUp))(
          note,
        );
        yield* sql`
          UPDATE orchestration_v2_projection_turn_items
          SET payload_json = json_set(payload_json, '$.headsUp', json(${encoded}))
          WHERE turn_item_id = ${row.turn_item_id}
        `;
      }
      after = row.turn_item_id;
    }
  }
  // Both changes now exist; reconcile the old fork ledger with upstream's ID.
  yield* sql`UPDATE effect_sql_migrations SET name = 'McpAppModelContext'
    WHERE migration_id = 59 AND name = 'HeadsUpInbox'`;
  yield* sql`UPDATE effect_sql_migrations SET name = 'ThreadSnapshotWindowIndexes'
    WHERE migration_id = 60 AND name = 'HeadsUpInbox'`;
  yield* sql`CREATE INDEX IF NOT EXISTS orchestration_v2_heads_up_identity_idx
    ON orchestration_v2_projection_turn_items (
      COALESCE(json_extract(payload_json, '$.headsUp.sourceThreadId'), thread_id),
      provider_thread_id, json_extract(payload_json, '$.headsUp.noteId')
    ) WHERE type = 'system_notice' AND json_extract(payload_json, '$.headsUp.noteId') IS NOT NULL`;
});
