import type {
  HeadsUpInboxEntry,
  OrchestrationV2HeadsUp,
  OrchestrationV2TurnItem,
  TurnItemId,
} from "@t3tools/contracts";

export interface PendingHeadsUp extends OrchestrationV2HeadsUp {
  readonly turnItemId: TurnItemId;
}

/** Unresolved notes in a single thread projection, in projection order. */
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

/** The note quoted into a follow-up draft. */
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

/** Append only: the original unsent draft, including its context references, stays intact. */
export function appendHeadsUpFollowUp(draft: string, entry: HeadsUpInboxEntry): string {
  const attribution = `You should know from "${entry.sourceThreadTitle}" (source thread ${entry.sourceThreadId}). Follow-up for "${entry.targetThreadTitle}" (target thread ${entry.targetThreadId}):`;
  const followUp = `${attribution}\n${headsUpChatMessage(entry.note)}\n\nCan you explain the impact and suggest a fix?`;
  return draft.length > 0 ? `${draft}\n\n${followUp}` : followUp;
}
