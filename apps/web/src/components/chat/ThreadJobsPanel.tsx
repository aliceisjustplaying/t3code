import {
  jobIsActive,
  jobStateLabel,
  threadJobs,
  type ThreadJob,
} from "@t3tools/client-runtime/jobs";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  AuthOrchestrationOperateScope,
  type EnvironmentId,
  type ThreadId,
  type TurnItemId,
} from "@t3tools/contracts";
import { useState } from "react";
import { useThreadProjection } from "../../state/entities";
import { readEnvironmentScope, useEnvironmentScope } from "../../state/session";
import { threadEnvironment } from "../../state/threads";
import { useThreadJobs } from "../../state/use-thread-jobs";
import { useTurnItemDetail } from "../../state/queries";
import { useAtomCommand } from "../../state/use-atom-command";
import { ThreadDetailsSection } from "./ThreadDetailsSection";
import { ThreadDetailsControl } from "./ThreadDetailsControl";
import { AgentElapsed } from "./AgentElapsed";
import { Button } from "../ui/button";
import { CollapsibleSectionHeader } from "../ui/collapsible-section-header";
import {
  AlertDialog,
  AlertDialogPopup,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogClose,
} from "../ui/alert-dialog";

type Target = { environmentId: EnvironmentId; threadId: ThreadId };
function useJobs(target: Target) {
  const items =
    useThreadProjection(scopeThreadRef(target.environmentId, target.threadId))?.projection
      .turnItems ?? [];
  return useThreadJobs(target, items);
}
function JobDuration({ job }: { job: ThreadJob }) {
  return (
    <AgentElapsed
      agent={{
        status: jobIsActive(job) ? "running" : "completed",
        startedAt: new Date(job.startedAt).toISOString(),
        completedAt: job.endedAt === null ? null : new Date(job.endedAt).toISOString(),
      }}
    />
  );
}
export function ThreadJobsPanel(props: Target & { onSelect: (id: TurnItemId) => void }) {
  const { jobs, hasMore, isPending, error, loadOlder, refresh } = useJobs(props);
  const [expanded, setExpanded] = useState(false);
  const [sectionExpanded, setSectionExpanded] = useState(false);
  if (!jobs.length && !isPending && !error) return null;
  const active = jobs.filter(jobIsActive).length;
  const olderCount = Math.max(0, jobs.length - 5);
  return (
    <ThreadDetailsSection headingId="thread-jobs-heading" title="Jobs" showHeading={false}>
      <CollapsibleSectionHeader
        expanded={sectionExpanded}
        onClick={() => setSectionExpanded(!sectionExpanded)}
        accessory={<span className="text-2xs text-muted-foreground">{active} running</span>}
      >
        Jobs{!sectionExpanded && ` (${jobs.length}${hasMore ? "+" : ""})`}
      </CollapsibleSectionHeader>
      {sectionExpanded && (
        <>
          {(expanded ? jobs : jobs.slice(0, 5)).map((job) => (
            <ThreadDetailsControl
              key={job.turnItemId}
              onClick={() => props.onSelect(job.turnItemId)}
              title={job.sourceTitle ? `Subagent: ${job.sourceTitle}` : job.name}
            >
              <span className="min-w-0 flex-1 truncate text-xs">
                {job.sourceThreadId ? "↳ " : ""}
                {job.name}
              </span>
              <span
                className={
                  job.state === "failed" || job.state === "timed_out"
                    ? "text-2xs text-destructive"
                    : "text-2xs text-muted-foreground"
                }
              >
                {jobStateLabel[job.state]}
              </span>
              <span className="text-2xs text-muted-foreground">
                <JobDuration job={job} />
              </span>
            </ThreadDetailsControl>
          ))}
          {isPending && <p className="text-xs text-muted-foreground">Loading jobs…</p>}
          {error && (
            <Button variant="ghost" onClick={refresh}>
              Retry loading jobs
            </Button>
          )}
          {olderCount > 0 && (
            <ThreadDetailsControl
              tone="muted"
              aria-expanded={expanded}
              onClick={() => setExpanded(!expanded)}
            >
              {expanded ? "Hide" : "Show"} older jobs ({olderCount})
            </ThreadDetailsControl>
          )}
          {hasMore && (
            <ThreadDetailsControl
              tone="muted"
              disabled={isPending}
              onClick={() => {
                setExpanded(true);
                loadOlder();
              }}
            >
              Load older jobs
            </ThreadDetailsControl>
          )}
        </>
      )}
    </ThreadDetailsSection>
  );
}

export function ThreadJobDetails(props: Target & { itemId: TurnItemId; onClose: () => void }) {
  const items =
    useThreadProjection(scopeThreadRef(props.environmentId, props.threadId))?.projection
      .turnItems ?? [];
  const liveJob = threadJobs(items).find((candidate) => candidate.turnItemId === props.itemId);
  const detail = useTurnItemDetail({ ...props, revision: liveJob?.revision ?? "selected" });
  const fetchedJob = threadJobs(detail.data?.item ? [detail.data.item] : [])[0];
  const job =
    liveJob && (!fetchedJob || liveJob.revision >= fetchedJob.revision) ? liveJob : fetchedJob;
  const stop = useAtomCommand(threadEnvironment.stopJob);
  const canStop = useEnvironmentScope(props.environmentId, AuthOrchestrationOperateScope);
  const [confirm, setConfirm] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  return (
    <div className="absolute inset-0 overflow-y-auto bg-background" aria-label="Job details">
      <div className="chat-composer-lane w-full">
        <div className="mx-auto max-w-(--chat-content-max-width) px-3 py-2">
          <Button variant="ghost" onClick={props.onClose}>
            ← Back to thread
          </Button>
          {detail.error && (
            <Button variant="ghost" onClick={detail.refresh}>
              Retry loading job
            </Button>
          )}
          {job ? (
            <>
              <p className="mt-2 text-xs text-muted-foreground">Jobs / Details</p>
              <div className="mt-2 flex items-center justify-between gap-3">
                <h2 className="text-xl font-semibold">{job.name}</h2>
                {job.state === "running" && (
                  <Button
                    variant="outline"
                    disabled={pending || !canStop}
                    onClick={() => setConfirm(true)}
                  >
                    Stop job
                  </Button>
                )}
              </div>
              <dl className="my-3 grid grid-cols-2 gap-x-3 gap-y-2 border-y border-border py-2 text-sm">
                <div>
                  <dt className="text-xs text-muted-foreground">Status</dt>
                  <dd role="status">{jobStateLabel[job.state]}</dd>
                </div>
                <div>
                  <dt className="text-xs text-muted-foreground">Duration</dt>
                  <dd>
                    <JobDuration job={job} />
                  </dd>
                </div>
                <div>
                  <dt className="text-xs text-muted-foreground">Source</dt>
                  <dd>{job.sourceTitle ? `Subagent · ${job.sourceTitle}` : "This thread"}</dd>
                </div>
                <div>
                  <dt className="text-xs text-muted-foreground">Result</dt>
                  <dd>
                    {job.exitCode !== null
                      ? `Exit ${job.exitCode}`
                      : (job.signal ??
                        (job.state === "lost" ? "Exit not observed" : "Not finished"))}
                  </dd>
                </div>
              </dl>
              <p className="mb-2 break-all text-xs text-muted-foreground">{job.cwd}</p>
              <div className="mb-2 flex justify-between text-xs">
                <h3>Output</h3>
                <span className="text-muted-foreground">Last 16,000 characters</span>
              </div>
              <pre className="max-h-[60vh] overflow-auto whitespace-pre-wrap break-all rounded-lg border border-border bg-muted/40 p-2 text-xs">
                $ {job.command}
                {"\n\n"}
                {detail.isPending
                  ? "Loading output…"
                  : detail.error
                    ? "Could not load output."
                    : fetchedJob?.output || "(no output yet)"}
              </pre>
              {error && (
                <p role="alert" className="mt-3 text-sm text-destructive">
                  {error}
                </p>
              )}
              <AlertDialog open={confirm} onOpenChange={setConfirm}>
                <AlertDialogPopup>
                  <AlertDialogHeader>
                    <AlertDialogTitle>Stop this job?</AlertDialogTitle>
                    <AlertDialogDescription>
                      Stop “{job.name}”? The thread and other jobs will keep running.
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <AlertDialogFooter>
                    <AlertDialogClose render={<Button variant="outline" />}>
                      Keep running
                    </AlertDialogClose>
                    <Button
                      disabled={pending || !canStop || !jobIsActive(job)}
                      onClick={() => {
                        if (
                          !readEnvironmentScope(props.environmentId, AuthOrchestrationOperateScope)
                        )
                          return;
                        setPending(true);
                        setError(null);
                        void stop({
                          environmentId: props.environmentId,
                          input: { threadId: props.threadId, turnItemId: job.turnItemId },
                        }).then((result) => {
                          setPending(false);
                          setConfirm(false);
                          if (result._tag !== "Success")
                            setError(
                              "Could not request job stop. The owning session may have ended.",
                            );
                        });
                      }}
                    >
                      Stop job
                    </Button>
                  </AlertDialogFooter>
                </AlertDialogPopup>
              </AlertDialog>
            </>
          ) : (
            <p className="mt-6">
              {detail.isPending
                ? "Loading job…"
                : detail.error
                  ? "Could not load job."
                  : "This job is no longer available."}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
