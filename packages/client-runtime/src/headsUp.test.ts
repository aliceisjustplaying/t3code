import { ThreadId, TurnItemId, type HeadsUpInboxEntry } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { appendHeadsUpFollowUp, headsUpChatMessage, pendingHeadsUps } from "./headsUp.ts";

const notice = (id: string, resolution?: "knew") =>
  ({
    type: "system_notice",
    id: TurnItemId.make(id),
    headsUp: {
      noteId: id,
      tag: "Heads up",
      line: `line ${id}`,
      ...(resolution ? { resolution } : {}),
    },
  }) as never;

describe("pendingHeadsUps", () => {
  it("keeps every unresolved note, oldest first", () => {
    const items = [
      notice("a", "knew"),
      notice("b"),
      { type: "system_notice" } as never,
      notice("c"),
    ];
    expect(pendingHeadsUps(items).map((note) => note.noteId)).toEqual(["b", "c"]);
  });
});

describe("headsUpChatMessage", () => {
  it("quotes the note, evidence and explanation like Pi", () => {
    expect(
      headsUpChatMessage({
        tag: "Heads up",
        line: "Uses pnpm.",
        evidence: "pnpm-lock.yaml",
        explanation: "**Why**\n\nBecause.",
      }),
    ).toBe(
      "Here is a note offered by a side agent:\n> Heads up · Uses pnpm.\n> Evidence: pnpm-lock.yaml\n>\n> **Why**\n>\n> Because.",
    );
  });
});

it("preserves an unsent draft verbatim while attributing a source notice to its parent follow-up", () => {
  const entry: HeadsUpInboxEntry = {
    id: "source-identity",
    threadId: ThreadId.make("source"),
    turnItemId: TurnItemId.make("note"),
    sourceThreadId: ThreadId.make("source"),
    sourceThreadTitle: "Cache review",
    targetThreadId: ThreadId.make("parent"),
    targetThreadTitle: "Fix caching",
    providerThreadId: null,
    createdAt: "2026-10-06T00:00:00.000Z",
    readAt: null,
    note: {
      noteId: "n1",
      tag: "Heads up",
      line: "Stale cache",
      evidence: "cache.ts:12",
      explanation: "The TTL is wrong.",
    },
  };
  const original = "Keep this draft\n@[file:cache.ts]  ";
  const draft = appendHeadsUpFollowUp(original, entry);
  expect(draft.startsWith(`${original}\n\n`)).toBe(true);
  expect(draft).toContain('from "Cache review" (source thread source)');
  expect(draft).toContain('Follow-up for "Fix caching" (target thread parent)');
  expect(draft).toContain("> Evidence: cache.ts:12");
  expect(draft).toContain("> The TTL is wrong.");
  expect(appendHeadsUpFollowUp("", entry)).not.toMatch(/^\s/);
  expect(entry.note.resolution).toBeUndefined();
});
