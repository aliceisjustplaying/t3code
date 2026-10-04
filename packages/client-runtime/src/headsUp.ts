import type {
  OrchestrationV2HeadsUp,
  OrchestrationV2TurnItem,
  TurnItemId,
} from "@t3tools/contracts";

export interface PendingHeadsUp extends OrchestrationV2HeadsUp {
  readonly turnItemId: TurnItemId;
}

/** Every unresolved heads-up note, oldest first. The first is the one the band acts on. */
export function pendingHeadsUps(
  items: ReadonlyArray<OrchestrationV2TurnItem>,
): ReadonlyArray<PendingHeadsUp> {
  const pending: PendingHeadsUp[] = [];
  for (const item of items) {
    if (item.type !== "system_notice" || item.headsUp === undefined) continue;
    if (item.headsUp.resolution !== undefined) continue;
    pending.push({ ...item.headsUp, turnItemId: item.id });
  }
  return pending;
}

/** The note quoted into the conversation, as Pi's "Chat in main session" sends it. */
export function headsUpChatMessage(
  note: Pick<OrchestrationV2HeadsUp, "tag" | "line" | "evidence" | "explanation">,
): string {
  const body = [
    `${note.tag} · ${note.line}`,
    ...(note.evidence ? [`Evidence: ${note.evidence}`] : []),
    ...(note.explanation ? ["", note.explanation] : []),
  ]
    .join("\n")
    .split("\n")
    .map((line) => (line === "" ? ">" : `> ${line}`))
    .join("\n");
  return `Here is a note offered by a side agent:\n${body}`;
}
