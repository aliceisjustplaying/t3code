import type { OrchestrationV2Job, OrchestrationV2TurnItem, TurnItemId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export interface ThreadJob extends OrchestrationV2Job {
  readonly turnItemId: TurnItemId;
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
