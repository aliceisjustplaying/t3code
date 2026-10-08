import type { OrchestrationV2Job, OrchestrationV2TurnItem, TurnItemId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export interface ThreadJob extends OrchestrationV2Job {
  readonly turnItemId: TurnItemId;
  readonly revision: string;
}

export const jobIsActive = (job: Pick<OrchestrationV2Job, "state">) =>
  job.state === "running" || job.state === "stopping";

export const jobStateLabel: Record<OrchestrationV2Job["state"], string> = {
  running: "Running",
  stopping: "Stopping",
  succeeded: "Succeeded",
  failed: "Failed",
  timed_out: "Timed out",
  stopped: "Stopped",
  lost: "Session ended",
};

export function threadJobs(items: ReadonlyArray<OrchestrationV2TurnItem>): ThreadJob[] {
  const jobs: ThreadJob[] = [];
  for (const item of items) {
    if (item.type !== "system_notice" || !item.job) continue;
    const lost = item.status === "cancelled" && jobIsActive(item.job);
    jobs.push({
      ...item.job,
      turnItemId: item.id,
      revision: DateTime.formatIso(item.updatedAt),
      ...(lost
        ? ({
            state: "lost",
            endedAt: DateTime.toEpochMillis(item.completedAt ?? item.updatedAt),
          } as const)
        : {}),
    });
  }
  return jobs.sort((a, b) => b.startedAt - a.startedAt || a.turnItemId.localeCompare(b.turnItemId));
}

/** Merge paged summaries with live rows without letting an older page undo completion. */
export function mergeJobItems(
  pages: ReadonlyArray<OrchestrationV2TurnItem>,
  live: ReadonlyArray<OrchestrationV2TurnItem>,
): OrchestrationV2TurnItem[] {
  const items = new Map<TurnItemId, OrchestrationV2TurnItem>();
  for (const item of [...pages, ...live]) {
    if (item.type !== "system_notice" || item.job === undefined) continue;
    const previous = items.get(item.id);
    const newer =
      previous === undefined ||
      DateTime.toEpochMillis(item.updatedAt) > DateTime.toEpochMillis(previous.updatedAt);
    const sameRevision =
      previous !== undefined &&
      DateTime.toEpochMillis(item.updatedAt) === DateTime.toEpochMillis(previous.updatedAt);
    const wouldUndoCompletion =
      previous?.type === "system_notice" &&
      previous.job !== undefined &&
      !(previous.status === "running" && jobIsActive(previous.job)) &&
      item.status === "running" &&
      jobIsActive(item.job);
    if (newer || (sameRevision && !wouldUndoCompletion)) {
      items.set(item.id, item);
    }
  }
  return [...items.values()];
}

/** Job identity/state changes invalidate summaries; output revisions only refresh open details. */
export function jobItemsRevision(items: ReadonlyArray<OrchestrationV2TurnItem>): string {
  return items
    .flatMap((item) =>
      item.type === "system_notice" && item.job !== undefined
        ? [item.id + ":" + item.status + ":" + item.job.state]
        : [],
    )
    .join("|");
}
