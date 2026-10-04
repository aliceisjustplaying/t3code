import type { OrchestrationV2HeadsUp, OrchestrationV2TurnItem } from "@t3tools/contracts";

/**
 * Reads a "you should know" note from notice text. The first line is
 * `[ysk:<id>] <tag> · <line> (<evidence>)`, where the evidence group is
 * optional; anything after it is a Markdown explanation. Claude hook messages
 * arrive as "<Hook> says: …". Other text is not a note.
 */
export function parseHeadsUpNotice(text: string): OrchestrationV2HeadsUp | undefined {
  const body = (/^\S+ says: ([\s\S]*)$/.exec(text)?.[1] ?? text).trim();
  const newline = body.indexOf("\n");
  const head = newline === -1 ? body : body.slice(0, newline);
  const note = /^\[ysk:([^\]\s]+)\]\s*(.+?)\s+·\s+(.+)$/.exec(head.trim());
  if (note === null) return undefined;
  const [, noteId = "", tag = "", rest = ""] = note;
  const split = /^(.*?\S)\s+\(([^()]+)\)$/.exec(rest.trim());
  const evidence = split?.[2]?.trim();
  const explanation = newline === -1 ? "" : body.slice(newline + 1).trim();
  return {
    noteId,
    tag: tag.trim(),
    line: (split?.[1] ?? rest).trim(),
    ...(evidence ? { evidence } : {}),
    ...(explanation ? { explanation } : {}),
  };
}

/** Marks a provider's system notice as a heads-up note when its text is one. */
export function withHeadsUp(item: OrchestrationV2TurnItem): OrchestrationV2TurnItem {
  if (item.type !== "system_notice" || item.headsUp !== undefined) return item;
  const headsUp = parseHeadsUpNotice(item.message);
  return headsUp === undefined ? item : { ...item, title: headsUp.line, headsUp };
}
