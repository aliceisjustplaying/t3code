import { TurnItemId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { headsUpChatMessage, pendingHeadsUps } from "./headsUp.ts";

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
