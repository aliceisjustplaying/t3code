import { ProjectId, ThreadId, TurnItemId, type HeadsUpInboxEntry } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  appendHeadsUpFollowUp,
  headsUpChatMessage,
  pendingHeadsUps,
  groupHeadsUpInbox,
} from "./headsUp.ts";

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

it("preserves an unsent draft and appends the original YSK quote without routing metadata or an invented request", () => {
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
  const quote =
    "Here is a note offered by a side agent:\n> Heads up · Stale cache\n> Evidence: cache.ts:12\n>\n> The TTL is wrong.";
  expect(draft).toBe(`${original}\n\n${quote}`);
  expect(appendHeadsUpFollowUp("", entry)).toBe(quote);
});

it("groups server-ordered projects without changing notice identity or ordering", () => {
  const entry = (id: string, project: string): HeadsUpInboxEntry => ({
    id,
    projectId: ProjectId.make(project),
    projectTitle: project,
    threadId: ThreadId.make(id),
    sourceThreadId: ThreadId.make(id),
    sourceThreadTitle: id,
    targetThreadId: ThreadId.make(id),
    targetThreadTitle: id,
    turnItemId: TurnItemId.make(id),
    providerThreadId: null,
    createdAt: "2026-10-06T00:00:00.000Z",
    readAt: null,
    note: { noteId: id, tag: "Heads up", line: id },
  });
  const items = [entry("b1", "B"), entry("b2", "B"), entry("a1", "A")];
  const groups = groupHeadsUpInbox(items);
  expect(groups.map((group) => [group.title, group.items.map((item) => item.id)])).toEqual([
    ["B", ["b1", "b2"]],
    ["A", ["a1"]],
  ]);
  expect(groups[0]?.items[0]).toBe(items[0]);
});
