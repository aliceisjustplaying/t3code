import type { OrchestrationV2TurnItem } from "@t3tools/contracts";
import { expect, it } from "vite-plus/test";

import { threadJobs } from "./jobs.ts";

it("orders jobs newest-started first regardless of status", () => {
  const items = [
    { id: "older-running", state: "running", startedAt: 10 },
    { id: "old-failure", state: "failed", startedAt: 20 },
    { id: "new-success", state: "succeeded", startedAt: 40 },
    { id: "recent-timeout", state: "timed_out", startedAt: 30 },
  ].map(({ id, ...job }) => ({
    id,
    type: "system_notice",
    status: "completed",
    job,
  })) as OrchestrationV2TurnItem[];

  expect(threadJobs(items).map((job) => job.turnItemId)).toEqual([
    "new-success",
    "recent-timeout",
    "old-failure",
    "older-running",
  ]);
});
