import { Dialog } from "@base-ui/react/dialog";
import { useAtomValue } from "@effect/atom-react";
import { appendHeadsUpFollowUp } from "@t3tools/client-runtime/heads-up";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type {
  EnvironmentId,
  HeadsUpInboxEntry,
  HeadsUpInboxInput,
  ThreadId,
} from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { ArrowLeftIcon, LightbulbIcon, XIcon } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";

import { environmentCatalog } from "../../connection/catalog";
import { useComposerDraftStore } from "../../composerDraftStore";
import { buildThreadRouteParams } from "../../threadRoutes";
import { headsUpInbox } from "../../state/headsUpInbox";
import { useEnvironmentQuery } from "../../state/query";
import { useAtomCommand } from "../../state/use-atom-command";
import ChatMarkdown from "../ChatMarkdown";
import { Button } from "../ui/button";
import { SheetBackdrop } from "../ui/sheet";

export function HeadsUpInbox({
  environmentId,
  threadId,
  onDraftReady,
}: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly onDraftReady: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [chatColumn, setChatColumn] = useState<HTMLElement | null>(null);
  const triggerRef = useCallback((element: HTMLButtonElement | null) => {
    if (element)
      setChatColumn(element.closest<HTMLElement>("[data-chat-header]")?.parentElement ?? null);
  }, []);
  const summary = useEnvironmentQuery(headsUpInbox.summary({ environmentId, input: {} }));
  const connection = useAtomValue(environmentCatalog.stateAtom(environmentId));
  const connected = Option.getOrNull(AsyncResult.value(connection))?.phase === "connected";
  const count = summary.data?.unresolvedCount;
  const unreadCount = summary.data?.unreadCount;
  const highlight = (unreadCount ?? 0) > 0;
  const label = `You should know${count === undefined ? ", loading" : `, ${count} unresolved, ${unreadCount ?? 0} unread`}${connected ? "" : ", disconnected"}`;
  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger
        render={
          <Button
            ref={triggerRef}
            variant={highlight ? "warning-outline" : "ghost"}
            size="sm"
            aria-label={label}
          />
        }
      >
        <LightbulbIcon aria-hidden="true" />
        {highlight ? <span className="text-xs font-medium">New</span> : null}
        <span
          className={`rounded px-1 text-xs tabular-nums transition-colors duration-300 motion-reduce:transition-none ${highlight ? "bg-warning/30 text-warning-foreground" : "bg-muted text-muted-foreground"}`}
        >
          {connected && !summary.error ? (count ?? "…") : "!"}
        </span>
      </Dialog.Trigger>
      {open ? (
        <Dialog.Portal container={chatColumn ?? undefined}>
          <SheetBackdrop className="z-60 sm:absolute sm:top-(--workspace-topbar-height) sm:z-(--z-sheet)" />
          <Dialog.Viewport className="fixed inset-0 z-60 flex justify-end sm:absolute sm:top-(--workspace-topbar-height) sm:z-(--z-sheet)">
            <Dialog.Popup className="flex h-full w-full min-w-0 flex-col overflow-hidden border-l border-border bg-popover text-popover-foreground shadow-lg sm:max-w-[450px]">
              <div className="flex flex-col gap-1 px-4 py-3">
                <div className="flex items-center gap-3">
                  <Dialog.Close
                    render={<Button variant="ghost" size="sm" aria-label="Back to chat" />}
                  >
                    <ArrowLeftIcon className="sm:hidden" aria-hidden="true" />
                    <span className="sm:hidden">Back</span>
                    <XIcon className="hidden sm:block" aria-hidden="true" />
                  </Dialog.Close>
                  <Dialog.Title className="text-xl font-semibold">You should know</Dialog.Title>
                </div>
                <Dialog.Description className="text-sm text-muted-foreground">
                  Current environment · All projects and threads
                </Dialog.Description>
              </div>
              <InboxPages
                key={`${environmentId}:${threadId}`}
                environmentId={environmentId}
                threadId={threadId}
                connected={connected}
                onClose={() => setOpen(false)}
                onDraftReady={onDraftReady}
              />
              <p className="border-t border-border px-4 py-3 text-xs text-muted-foreground">
                Opening marks notices read, not resolved.
              </p>
            </Dialog.Popup>
          </Dialog.Viewport>
        </Dialog.Portal>
      ) : null}
    </Dialog.Root>
  );
}

function InboxPages({
  environmentId,
  threadId,
  connected,
  onClose,
  onDraftReady,
}: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly onDraftReady: () => void;
  readonly connected: boolean;
  readonly onClose: () => void;
}) {
  const [view, setView] = useState<HeadsUpInboxInput["view"]>("unresolved");
  const [cursor, setCursor] = useState<string | undefined>();
  const [expanded, setExpanded] = useState<string | null>(null);
  const [feedback, setFeedback] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [readError, setReadError] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const attemptedReads = useRef(new Set<string>());
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const summary = useEnvironmentQuery(headsUpInbox.summary({ environmentId, input: {} }));
  const page = useEnvironmentQuery(
    headsUpInbox.page({ environmentId, input: { view, limit: 30, ...(cursor ? { cursor } : {}) } }),
  );
  const read = useAtomCommand(headsUpInbox.read, { reportFailure: false });
  const resolve = useAtomCommand(headsUpInbox.resolve, { reportFailure: false });
  const navigate = useNavigate();
  const items = page.data?.items;
  useEffect(() => {
    if (!connected || !items) return;
    const unread = items.filter(
      (entry) => entry.readAt === null && !attemptedReads.current.has(entry.id),
    );
    for (const entry of unread) attemptedReads.current.add(entry.id);
    void Promise.all(
      unread.map((entry) =>
        read({ environmentId, input: { threadId: entry.threadId, turnItemId: entry.turnItemId } }),
      ),
    )
      .then((results) => {
        if (mounted.current && results.some((result) => result._tag !== "Success"))
          setReadError(true);
      })
      .catch(() => {
        if (mounted.current) setReadError(true);
      });
  }, [connected, environmentId, items, read]);
  const changePage = (nextView: HeadsUpInboxInput["view"], nextCursor?: string) => {
    setView(nextView);
    setCursor(nextCursor);
    setExpanded(null);
    setActionError(null);
    setFeedback(null);
  };
  const act = async (entry: HeadsUpInboxEntry, resolution: "dismiss" | "knew" | null) => {
    setBusy(entry.id);
    setActionError(null);
    try {
      const result = await resolve({
        environmentId,
        input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution },
      });
      if (!mounted.current) return;
      if (result._tag !== "Success") setActionError("The notice could not be updated. Try again.");
      else {
        setExpanded(null);
        setFeedback(
          resolution === null
            ? "Restored to Unresolved."
            : `${resolution === "knew" ? "Marked as known" : "Dismissed"}. Available in Reviewed.`,
        );
        page.refresh();
      }
    } catch {
      if (mounted.current) setActionError("The notice could not be updated. Try again.");
    } finally {
      if (mounted.current) setBusy(null);
    }
  };
  const ask = async (entry: HeadsUpInboxEntry) => {
    setBusy(entry.id);
    setActionError(null);
    try {
      const result = await resolve({
        environmentId,
        input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution: "dismiss" },
      });
      if (result._tag !== "Success") {
        if (mounted.current) setActionError("The notice could not be updated. Try again.");
        return;
      }
      const target = scopeThreadRef(environmentId, entry.targetThreadId);
      const store = useComposerDraftStore.getState();
      store.setPrompt(
        target,
        appendHeadsUpFollowUp(store.getComposerDraft(target)?.prompt ?? "", entry),
      );
      // Closing the inbox must not discard a draft for an already-dismissed notice.
      if (!mounted.current) return;
      onDraftReady();
      onClose();
      if (threadId !== entry.targetThreadId)
        void navigate({ to: "/$environmentId/$threadId", params: buildThreadRouteParams(target) });
    } catch {
      if (mounted.current) setActionError("The notice could not be updated. Try again.");
    } finally {
      if (mounted.current) setBusy(null);
    }
  };
  const loadError = page.error ?? summary.error;
  return (
    <>
      <div className="flex gap-1 border-b border-border px-4" aria-label="Inbox view">
        {(["unresolved", "reviewed"] as const).map((tab) => (
          <button
            key={tab}
            type="button"
            aria-pressed={view === tab}
            onClick={() => changePage(tab)}
            className="min-h-11 px-3 text-sm capitalize aria-pressed:border-b-2 aria-pressed:border-warning focus-visible:outline-2 focus-visible:outline-ring"
          >
            {tab}{" "}
            {summary.data
              ? `(${tab === "unresolved" ? summary.data.unresolvedCount : summary.data.reviewedCount})`
              : ""}
          </button>
        ))}
      </div>
      <div
        className="min-h-0 min-w-0 flex-1 overflow-x-hidden overflow-y-auto [overflow-wrap:anywhere]"
        key={`${view}:${cursor ?? ""}`}
        aria-busy={page.isPending}
      >
        {!connected ? (
          <p role="status" className="p-4 text-sm text-muted-foreground">
            Disconnected. Reconnect to refresh or update notices.
          </p>
        ) : null}
        {loadError ? (
          <div role="alert" className="p-4 text-sm">
            <p>{loadError}</p>
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                page.refresh();
                summary.refresh();
              }}
            >
              Retry
            </Button>
          </div>
        ) : null}
        {/* Read acknowledgments revalidate this page; keep loaded rows in place. */}
        {!page.data && !loadError ? (
          <p role="status" className="p-4 text-sm text-muted-foreground">
            Loading notices…
          </p>
        ) : null}
        {connected && !loadError && page.data?.items.length === 0 ? (
          <p className="p-6 text-sm text-muted-foreground">
            {cursor
              ? "No notices on this page."
              : view === "reviewed"
                ? "No reviewed notices yet."
                : "Nothing unresolved. Reviewed notices can be restored anytime."}
          </p>
        ) : null}
        {items?.map((entry) => (
          <section key={entry.id} className="border-b border-border">
            <button
              type="button"
              aria-expanded={expanded === entry.id}
              aria-controls={`ysk-${entry.id}`}
              onClick={() => setExpanded(expanded === entry.id ? null : entry.id)}
              className="block w-full px-4 py-2 text-left hover:bg-muted/50 focus-visible:outline-2 focus-visible:outline-ring aria-expanded:bg-muted/50"
            >
              <span className="flex flex-wrap justify-between gap-3 text-xs text-muted-foreground">
                <span>
                  {entry.note.tag}
                  {entry.readAt === null ? " · Unread" : ""}
                </span>
                <time dateTime={entry.createdAt}>{new Date(entry.createdAt).toLocaleString()}</time>
              </span>
              <span
                className={`mt-1 text-sm font-medium ${expanded === entry.id ? "block" : "line-clamp-2 sm:line-clamp-none"}`}
              >
                {entry.note.line}
              </span>
            </button>
            <div className="flex flex-wrap items-center gap-x-3 px-4 pb-2">
              <div className="min-w-0 flex-1 break-words">
                <span
                  className={`block text-xs text-muted-foreground ${expanded === entry.id ? "" : "max-sm:truncate"}`}
                >
                  From {entry.sourceThreadTitle}
                </span>
                {entry.note.resolution ? (
                  <span className="block text-xs text-muted-foreground">
                    {entry.note.resolution === "knew"
                      ? "Knew"
                      : entry.note.resolution === "dismiss"
                        ? "Dismissed"
                        : "Reviewed"}
                  </span>
                ) : null}
              </div>
              <div
                className="flex min-h-11 w-full flex-wrap items-center gap-3"
                aria-busy={busy === entry.id}
              >
                <Button
                  size="sm"
                  disabled={!connected || busy !== null}
                  aria-label={`Ask agent · Draft: ${entry.note.line}`}
                  onClick={(event) => {
                    event.stopPropagation();
                    void ask(entry);
                  }}
                >
                  Ask agent · Draft
                </Button>
                {view === "reviewed" ? (
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={!connected || busy !== null}
                    aria-label={`${entry.note.resolution === "dismiss" || entry.note.resolution === "knew" ? "Undo" : "Restore"} · Restore to unresolved: ${entry.note.line}`}
                    onClick={(event) => {
                      event.stopPropagation();
                      void act(entry, null);
                    }}
                  >
                    {entry.note.resolution === "dismiss" || entry.note.resolution === "knew"
                      ? "Undo"
                      : "Restore"}
                  </Button>
                ) : (
                  <>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={!connected || busy !== null}
                      aria-label={`Dismiss: ${entry.note.line}`}
                      onClick={(event) => {
                        event.stopPropagation();
                        void act(entry, "dismiss");
                      }}
                    >
                      Dismiss
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={!connected || busy !== null}
                      aria-label={`Knew: ${entry.note.line}`}
                      onClick={(event) => {
                        event.stopPropagation();
                        void act(entry, "knew");
                      }}
                    >
                      Knew
                    </Button>
                  </>
                )}
              </div>
              {busy === entry.id ? (
                <p role="status" className="text-xs">
                  Saving…
                </p>
              ) : null}
            </div>
            {expanded === entry.id ? (
              <div
                id={`ysk-${entry.id}`}
                className="min-w-0 space-y-2 bg-muted/20 px-4 py-2 text-sm"
              >
                {entry.note.explanation ? (
                  <ChatMarkdown
                    text={entry.note.explanation}
                    cwd={undefined}
                    environmentId={environmentId}
                  />
                ) : (
                  <p className="text-muted-foreground">No additional explanation was supplied.</p>
                )}
                <p className="text-xs text-muted-foreground">
                  Source: {entry.sourceThreadTitle}
                  <br />
                  Draft target: {entry.targetThreadTitle}
                </p>
                {entry.note.evidence ? (
                  <>
                    <h3 className="text-xs font-medium">Evidence</h3>
                    <pre className="max-w-full whitespace-pre-wrap [overflow-wrap:anywhere] rounded border border-border p-3 text-xs">
                      {entry.note.evidence}
                    </pre>
                  </>
                ) : null}
                <p className="text-xs text-muted-foreground">
                  Ask agent adds the note to the target thread’s unsent draft and dismisses it from
                  Unresolved. Nothing is sent.
                </p>
              </div>
            ) : null}
          </section>
        ))}
        <div className="flex gap-2 p-4">
          {cursor ? (
            <Button variant="outline" size="sm" onClick={() => changePage(view)}>
              Newest
            </Button>
          ) : null}
          {page.data?.nextCursor ? (
            <Button
              variant="outline"
              size="sm"
              disabled={!connected || page.isPending}
              onClick={() => changePage(view, page.data?.nextCursor ?? undefined)}
            >
              Older notices
            </Button>
          ) : null}
        </div>
      </div>
      {readError ? (
        <div role="alert" className="px-4 py-2 text-xs">
          <p>Some notices could not be marked read.</p>
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              attemptedReads.current.clear();
              setReadError(false);
              page.refresh();
            }}
          >
            Retry marking read
          </Button>
        </div>
      ) : null}
      {actionError ? (
        <p role="alert" className="px-4 py-2 text-sm text-destructive">
          {actionError}
        </p>
      ) : null}
      {feedback ? (
        <p role="status" className="px-4 py-2 text-xs text-muted-foreground">
          {feedback}
        </p>
      ) : null}
    </>
  );
}
