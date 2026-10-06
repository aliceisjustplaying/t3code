import * as Schema from "effect/Schema";
import {
  IsoDateTime,
  NonNegativeInt,
  ProviderThreadId,
  ThreadId,
  TurnItemId,
} from "./baseSchemas.ts";
import { OrchestrationV2HeadsUp } from "./orchestrationV2.ts";

export const HeadsUpInboxInput = Schema.Struct({
  view: Schema.Literals(["unresolved", "reviewed"]),
  cursor: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(4096))),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
});
export type HeadsUpInboxInput = typeof HeadsUpInboxInput.Type;

export const HeadsUpInboxEntry = Schema.Struct({
  /** Stable source identity, shared by a child notice and its forwarded copies. */
  id: Schema.String,
  threadId: ThreadId,
  turnItemId: TurnItemId,
  sourceThreadId: ThreadId,
  sourceThreadTitle: Schema.String,
  targetThreadId: ThreadId,
  targetThreadTitle: Schema.String,
  providerThreadId: Schema.NullOr(ProviderThreadId),
  createdAt: IsoDateTime,
  readAt: Schema.NullOr(IsoDateTime),
  note: OrchestrationV2HeadsUp,
});
export type HeadsUpInboxEntry = typeof HeadsUpInboxEntry.Type;

export const HeadsUpInboxSummary = Schema.Struct({
  sequence: NonNegativeInt,
  unreadCount: NonNegativeInt,
  unresolvedCount: NonNegativeInt,
  reviewedCount: NonNegativeInt,
});
export type HeadsUpInboxSummary = typeof HeadsUpInboxSummary.Type;
export const HeadsUpInboxPage = Schema.Struct({
  ...HeadsUpInboxSummary.fields,
  items: Schema.Array(HeadsUpInboxEntry),
  nextCursor: Schema.NullOr(Schema.String),
});
export type HeadsUpInboxPage = typeof HeadsUpInboxPage.Type;
export class HeadsUpInboxError extends Schema.TaggedError<HeadsUpInboxError>()(
  "HeadsUpInboxError",
  {
    operation: Schema.Literals(["query", "cursor", "decode", "subscribe"]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Could not load the You should know inbox.";
  }
}
