import { jobItemsRevision, mergeJobItems, threadJobs } from "@t3tools/client-runtime/jobs";
import type {
  EnvironmentId,
  OrchestrationV2GetJobsPageInput,
  OrchestrationV2GetJobsPageResult,
  OrchestrationV2TurnItem,
  ThreadId,
} from "@t3tools/contracts";
import { useState } from "react";
import { orchestrationEnvironment } from "../../state/orchestration";
import { useEnvironmentQuery } from "../../state/query";

/** Pages belong to the destination environment and never seed another thread. */
export function useThreadJobs(
  target: { environmentId: EnvironmentId; threadId: ThreadId } | null,
  liveItems: ReadonlyArray<OrchestrationV2TurnItem>,
) {
  const revision = jobItemsRevision(liveItems);
  const key =
    target === null ? "" : JSON.stringify([target.environmentId, target.threadId, revision]);
  // Keep the newest page subscribed even while browsing older history. Its
  // reconnect/refresh response replaces accumulated pages, not just the cursor page.
  const first = useEnvironmentQuery(
    target === null
      ? null
      : orchestrationEnvironment.jobsPage({
          environmentId: target.environmentId,
          input: { threadId: target.threadId, cursor: null, revision },
        }),
  );
  const [stored, setStored] = useState<{
    key: string;
    first: OrchestrationV2GetJobsPageResult | null;
    cursor: OrchestrationV2GetJobsPageInput["cursor"];
    items: OrchestrationV2TurnItem[];
    page: OrchestrationV2GetJobsPageResult | null;
  }>({ key, first: first.data, cursor: null, items: [], page: null });
  const current =
    stored.key === key && stored.first === first.data
      ? stored
      : { key, first: first.data, cursor: null, items: [], page: null };
  const older = useEnvironmentQuery(
    target === null || current.cursor == null
      ? null
      : orchestrationEnvironment.jobsPage({
          environmentId: target.environmentId,
          input: { threadId: target.threadId, cursor: current.cursor, revision },
        }),
  );
  if (current !== stored) {
    setStored(current);
  } else if (older.data !== null && stored.page !== older.data) {
    setStored({
      ...current,
      items: mergeJobItems(current.items, older.data.items),
      page: older.data,
    });
  }
  const items = mergeJobItems(
    mergeJobItems(current.items, [...(first.data?.items ?? []), ...(older.data?.items ?? [])]),
    liveItems,
  );
  const page = current.cursor == null ? first : older;
  return {
    jobs: threadJobs(items),
    hasMore: page.data?.nextCursor != null,
    isPending: first.isPending || page.isPending,
    error: first.error ?? page.error,
    refresh: () => {
      setStored({ key, first: first.data, cursor: null, items: [], page: null });
      first.refresh();
    },
    loadOlder: () => {
      const cursor = page.data?.nextCursor;
      if (cursor == null || first.isPending || page.isPending) return;
      setStored({ ...current, cursor, items, page: null });
    },
  };
}
