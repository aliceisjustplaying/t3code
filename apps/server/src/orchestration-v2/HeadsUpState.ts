import { OrchestrationV2HeadsUpAction, type OrchestrationV2TurnItem } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { withHeadsUp } from "./HeadsUpNotice.ts";

const decodeAction = Schema.decodeUnknownEffect(OrchestrationV2HeadsUpAction);

/** Inherit user-owned inbox state inside the event write/replay transaction. */
export const inheritHeadsUpState = Effect.fnUntraced(function* (input: OrchestrationV2TurnItem) {
  let item = withHeadsUp(input);
  if (item.type === "system_notice" && item.headsUp !== undefined) {
    const sql = yield* SqlClient.SqlClient;
    const state = yield* sql<{ readAt: string | null; resolution: string | null }>`
      SELECT MAX(json_extract(payload_json, '$.headsUp.readAt')) AS "readAt",
        MAX(json_extract(payload_json, '$.headsUp.resolution')) AS resolution
      FROM orchestration_v2_projection_turn_items
      WHERE type = 'system_notice'
        AND json_extract(payload_json, '$.headsUp.noteId') = ${item.headsUp.noteId}
        AND COALESCE(json_extract(payload_json, '$.headsUp.sourceThreadId'), thread_id) = ${item.headsUp.sourceThreadId ?? item.threadId}
        AND provider_thread_id IS ${item.providerThreadId}
    `;
    // Providers can repeat a note with a new item id on reconnect.
    // Its source identity retains the user's read/review state.
    const prior = state[0];
    const resolution =
      prior?.resolution == null ? undefined : yield* decodeAction(prior.resolution);
    item = {
      ...item,
      headsUp: {
        ...item.headsUp,
        ...(prior?.readAt == null ? {} : { readAt: prior.readAt }),
        ...(resolution === undefined ? {} : { resolution }),
      },
    };
  }

  return item;
});
