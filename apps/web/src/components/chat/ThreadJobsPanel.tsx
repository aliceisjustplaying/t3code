import {
  jobIsActive,
  jobStateLabel,
  threadJobs,
  type ThreadJob,
} from "@t3tools/client-runtime/jobs";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId, ThreadId, TurnItemId } from "@t3tools/contracts";
import { useState } from "react";
import { useThreadProjection } from "../../state/entities";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { ThreadDetailsSection } from "./ThreadDetailsSection";
import { ThreadDetailsControl } from "./ThreadDetailsControl";
import { AgentElapsed } from "./AgentElapsed";
import { Button } from "../ui/button";
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
  return threadJobs(
    useThreadProjection(scopeThreadRef(target.environmentId, target.threadId))?.projection
      .turnItems ?? [],
  );
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
  const jobs = useJobs(props);
  const [finished, setFinished] = useState(false);
  if (!jobs.length) return null;
  const active = jobs.filter(jobIsActive).length;
  const finishedCount = jobs.filter(
    (job) => !jobIsActive(job) && job.state !== "failed" && job.state !== "timed_out",
  ).length;
  return (
    <ThreadDetailsSection
      headingId="thread-jobs-heading"
      title="Jobs"
      actions={<span className="text-2xs text-muted-foreground">{active} running</span>}
    >
      {jobs
        .filter(
          (job) =>
            finished || jobIsActive(job) || job.state === "failed" || job.state === "timed_out",
        )
        .map((job) => (
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
      {finishedCount > 0 && (
        <ThreadDetailsControl
          tone="muted"
          aria-expanded={finished}
          onClick={() => setFinished(!finished)}
        >
          {finished ? "Hide" : "Show"} finished ({finishedCount})
        </ThreadDetailsControl>
      )}
    </ThreadDetailsSection>
  );
}

export function ThreadJobDetails(props: Target & { itemId: TurnItemId; onClose: () => void }) {
  const job = useJobs(props).find((candidate) => candidate.turnItemId === props.itemId);
  const stop = useAtomCommand(threadEnvironment.stopJob);
  const [confirm, setConfirm] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  return (
    <div className="absolute inset-0 overflow-y-auto bg-background" aria-label="Job details">
      <div className="chat-composer-lane w-full">
        <div className="mx-auto max-w-(--chat-content-max-width) px-5 py-8">
          <Button variant="ghost" onClick={props.onClose}>
            ← Back to thread
          </Button>
          {job ? (
            <>
              <p className="mt-6 text-xs text-muted-foreground">Jobs / Details</p>
              <div className="mt-2 flex items-center justify-between gap-3">
                <h2 className="text-xl font-semibold">{job.name}</h2>
                {job.state === "running" && (
                  <Button variant="outline" disabled={pending} onClick={() => setConfirm(true)}>
                    Stop job
                  </Button>
                )}
              </div>
              <dl className="my-6 grid grid-cols-2 gap-5 border-y border-border py-5 text-sm">
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
              <pre className="max-h-[60vh] overflow-auto whitespace-pre-wrap break-all rounded-lg border border-border bg-muted/40 p-4 text-xs">
                $ {job.command}
                {"\n\n"}
                {job.output || "(no output yet)"}
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
                      disabled={pending || !jobIsActive(job)}
                      onClick={() => {
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
            <p className="mt-6">This job is no longer available.</p>
          )}
        </div>
      </div>
    </div>
  );
}
