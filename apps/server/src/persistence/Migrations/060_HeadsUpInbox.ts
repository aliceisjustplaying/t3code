import { OrchestrationV2HeadsUp } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { parseHeadsUpNotice } from "../../orchestration-v2/HeadsUpNotice.ts";

import migrateMcpAppModelContext from "./059_McpAppModelContext.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Existing fork databases used migration 59 for YSK, before upstream used
  // that ID for MCP apps. Apply its idempotent schema change here as well.
  yield* migrateMcpAppModelContext;
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
  yield* sql`CREATE INDEX IF NOT EXISTS orchestration_v2_heads_up_identity_idx
    ON orchestration_v2_projection_turn_items (
      COALESCE(json_extract(payload_json, '$.headsUp.sourceThreadId'), thread_id),
      provider_thread_id, json_extract(payload_json, '$.headsUp.noteId')
    ) WHERE type = 'system_notice' AND json_extract(payload_json, '$.headsUp.noteId') IS NOT NULL`;
});
