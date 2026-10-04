import {
  headsUpChatMessage,
  pendingHeadsUps,
  type PendingHeadsUp,
} from "@t3tools/client-runtime/heads-up";
import type {
  EnvironmentId,
  OrchestrationV2HeadsUpAction,
  OrchestrationV2TurnItem,
  ProviderInteractionMode,
  RuntimeMode,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { useCallback, useMemo, useState, type KeyboardEvent } from "react";

import { cn, newMessageId } from "~/lib/utils";

import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import ChatMarkdown from "../ChatMarkdown";
import { Button } from "../ui/button";
import {
  Dialog,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { Kbd } from "../ui/kbd";
import { stackedThreadToast, toastManager } from "../ui/toast";
import type { ComposerBannerStackItem } from "./ComposerBannerStack";

/**
 * Heads-up notes ("you should know") pinned above the composer. The oldest open
 * note leads the composer banners; "+N more" lists the rest. A note stays until
 * the user answers it here, and every answer but "Ask agent" can be undone.
 * Returns the composer's banner items with the note added, and the dialogs to mount.
 */
export function useHeadsUpBand(input: {
  readonly environmentId: EnvironmentId | null;
  readonly threadId: ThreadId | null;
  readonly turnItems: ReadonlyArray<OrchestrationV2TurnItem> | null;
  readonly cwd: string | undefined;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly bannerItems: ReadonlyArray<ComposerBannerStackItem>;
}) {
  const { environmentId, threadId, turnItems, cwd, runtimeMode, interactionMode } = input;
  // Notes the user just answered leave at once instead of waiting for the server echo.
  const [answered, setAnswered] = useState<ReadonlySet<TurnItemId>>(() => new Set());
  const notes = useMemo(
    () => pendingHeadsUps(turnItems ?? []).filter((note) => !answered.has(note.turnItemId)),
    [answered, turnItems],
  );
  const top = notes[0] ?? null;
  const resolveHeadsUp = useAtomCommand(threadEnvironment.resolveHeadsUp);
  const startTurn = useAtomCommand(threadEnvironment.startTurn);
  const [explained, setExplained] = useState<PendingHeadsUp | null>(null);
  const [triageOpen, setTriageOpen] = useState(false);
  const [focusIndex, setFocusIndex] = useState(0);

  const setResolution = useCallback(
    (ids: ReadonlyArray<TurnItemId>, resolution: OrchestrationV2HeadsUpAction | null) => {
      if (environmentId === null || threadId === null) return;
      const mark = (id: TurnItemId, done: boolean) =>
        setAnswered((current) => {
          const next = new Set(current);
          if (done) next.add(id);
          else next.delete(id);
          return next;
        });
      for (const turnItemId of ids) {
        mark(turnItemId, resolution !== null);
        void resolveHeadsUp({ environmentId, input: { threadId, turnItemId, resolution } }).then(
          (result) => {
            if (result._tag !== "Success" && resolution !== null) mark(turnItemId, false);
          },
        );
      }
    },
    [environmentId, resolveHeadsUp, threadId],
  );

  const offerUndo = useCallback(
    (ids: ReadonlyArray<TurnItemId>, title: string) => {
      const toastId = toastManager.add(
        stackedThreadToast({
          type: "info",
          title,
          actionProps: {
            children: "Undo",
            onClick: () => {
              toastManager.close(toastId);
              setResolution(ids, null);
            },
          },
        }),
      );
    },
    [setResolution],
  );

  // Queued like a follow-up, so it waits behind any active run.
  const askAgent = useCallback(
    (note: PendingHeadsUp) => {
      if (environmentId === null || threadId === null) return;
      setResolution([note.turnItemId], "send");
      void startTurn({
        environmentId,
        input: {
          threadId,
          message: {
            messageId: newMessageId(),
            role: "user",
            text: headsUpChatMessage(note),
            attachments: [],
          },
          runtimeMode,
          interactionMode,
          dispatchMode: "queue",
        },
      });
    },
    [environmentId, interactionMode, runtimeMode, setResolution, startTurn, threadId],
  );

  const act = useCallback(
    (note: PendingHeadsUp, action: OrchestrationV2HeadsUpAction) => {
      if (action === "send") return askAgent(note);
      setResolution([note.turnItemId], action);
      if (action === "learn") setExplained(note);
      if (action === "dismiss") offerUndo([note.turnItemId], "Heads-up dismissed");
      if (action === "knew") offerUndo([note.turnItemId], "Marked as known");
    },
    [askAgent, offerUndo, setResolution],
  );

  const openTriage = useCallback(() => {
    setFocusIndex(0);
    setTriageOpen(true);
  }, []);

  const bannerItems = useMemo<ReadonlyArray<ComposerBannerStackItem>>(() => {
    if (top === null) return input.bannerItems;
    const more = notes.length - 1;
    const item: ComposerBannerStackItem = {
      id: "heads-up",
      variant: "info",
      icon: <span className="text-primary">{"\u2726"}</span>,
      title: <NoteText note={top} primary />,
      actions: (
        <>
          {more > 0 ? (
            <Button size="xs" variant="ghost" onClick={openTriage}>
              +{more} more
            </Button>
          ) : null}
          {noteActions(top).map(([action, label]) => (
            <Button key={action} size="xs" variant="ghost" onClick={() => act(top, action)}>
              {label}
            </Button>
          ))}
        </>
      ),
      dismissLabel: "Dismiss heads-up",
      onDismiss: () => act(top, "dismiss"),
    };
    return [item, ...input.bannerItems];
  }, [act, input.bannerItems, notes.length, openTriage, top]);

  const focused = notes[Math.min(focusIndex, notes.length - 1)] ?? null;
  const onTriageKey = (event: KeyboardEvent) => {
    if (focused === null || event.metaKey || event.ctrlKey || event.altKey) return;
    const last = notes.length - 1;
    const run =
      event.key === "ArrowDown"
        ? () => setFocusIndex(Math.min(last, focusIndex + 1))
        : event.key === "ArrowUp"
          ? () => setFocusIndex(Math.max(0, focusIndex - 1))
          : (() => {
              const action = noteActions(focused).find(([, , key]) => key === event.key)?.[0];
              return action === undefined ? null : () => act(focused, action);
            })();
    if (run === null) return;
    event.preventDefault();
    run();
  };
  const closeExplanation = () => setExplained(null);
  const onExplanationKey = (event: KeyboardEvent) => {
    if (explained === null || event.key !== "a" || event.metaKey || event.ctrlKey) return;
    event.preventDefault();
    closeExplanation();
    askAgent(explained);
  };

  const overlay = (
    <>
      <Dialog
        open={triageOpen && focused !== null}
        onOpenChange={(next) => (next ? undefined : setTriageOpen(false))}
      >
        {triageOpen && focused !== null ? (
          <DialogPopup className="max-w-xl" onKeyDown={onTriageKey}>
            <DialogHeader>
              <DialogTitle>
                {"\u2726"} {notes.length} open heads-up{notes.length === 1 ? "" : "s"}
              </DialogTitle>
            </DialogHeader>
            {/* Fixed sizes: removing notes or moving the pointer never reflows. */}
            <div className="flex h-[22rem] flex-col gap-3 px-6 pb-2">
              <ul
                className="min-h-0 flex-1 overflow-y-auto"
                role="listbox"
                aria-label="Open heads-ups"
              >
                {notes.map((note, index) => (
                  <li
                    key={note.turnItemId}
                    role="option"
                    aria-selected={note === focused}
                    onClick={() => setFocusIndex(index)}
                    className={cn(
                      "flex h-8 cursor-default items-center rounded-md px-2 text-sm",
                      note === focused && "bg-accent",
                    )}
                  >
                    <NoteText note={note} primary={note === focused} />
                  </li>
                ))}
              </ul>
              <div className="h-24 shrink-0 overflow-y-auto rounded-md border px-3 py-2 text-sm">
                <p>
                  <InlineNoteText text={focused.line} />
                </p>
                {focused.evidence ? (
                  <p className="mt-1 text-muted-foreground text-xs">
                    <InlineNoteText text={focused.evidence} />
                  </p>
                ) : null}
              </div>
            </div>
            <DialogFooter variant="bare" className="sm:justify-start">
              <span className="flex flex-wrap items-center gap-1">
                {noteActions(focused).map(([action, label, key]) => (
                  <Button
                    key={action}
                    size="xs"
                    variant="ghost"
                    onClick={() => act(focused, action)}
                  >
                    {label}
                    <Kbd>{key}</Kbd>
                  </Button>
                ))}
              </span>
            </DialogFooter>
          </DialogPopup>
        ) : null}
      </Dialog>
      <Dialog
        open={explained !== null}
        onOpenChange={(next) => (next ? undefined : closeExplanation())}
      >
        {explained === null ? null : (
          <DialogPopup className="max-w-xl" onKeyDown={onExplanationKey}>
            <DialogHeader>
              <DialogTitle className="sr-only">
                {explained.tag}: {explained.line}
              </DialogTitle>
              <NoteHeader note={explained} className="pe-8" />
            </DialogHeader>
            <DialogPanel>
              <ChatMarkdown text={explained.explanation ?? ""} cwd={cwd} />
            </DialogPanel>
            <DialogFooter>
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  closeExplanation();
                  askAgent(explained);
                }}
              >
                Ask agent
                <Kbd>a</Kbd>
              </Button>
              <Button size="sm" onClick={closeExplanation}>
                Understood
              </Button>
            </DialogFooter>
          </DialogPopup>
        )}
      </Dialog>
    </>
  );

  return { bannerItems, overlay };
}

/** The answers a note offers, with their key in the triage list; mirrors Pi's /ysk menu. */
function noteActions(note: PendingHeadsUp) {
  const actions: Array<readonly [OrchestrationV2HeadsUpAction, string, string]> = [];
  if (note.explanation) actions.push(["learn", "Explain", "e"]);
  actions.push(["knew", "Knew", "k"], ["send", "Ask agent", "a"], ["dismiss", "Dismiss", "x"]);
  return actions;
}

/** Renders `code spans` in a note; the rest stays plain text. */
function InlineNoteText({ text }: { text: string }) {
  const parts = text.split(/(`[^`]+`)/);
  return (
    <>
      {parts.map((part, index) => {
        if (part.length === 0) return null;
        // Offsets are unique once empty splits (a leading code span) are skipped.
        const key = parts.slice(0, index).join("").length;
        return part.length > 2 && part.startsWith("`") && part.endsWith("`") ? (
          <code key={key} className="rounded bg-muted px-1 font-mono text-foreground">
            {part.slice(1, -1)}
          </code>
        ) : (
          <span key={key}>{part}</span>
        );
      })}
    </>
  );
}

function NoteText({ note, primary = false }: { note: PendingHeadsUp; primary?: boolean }) {
  return (
    <span className="flex min-w-0 items-baseline gap-1">
      <span className={primary ? "shrink-0 font-medium" : "shrink-0 text-muted-foreground"}>
        {note.tag}
      </span>
      <span className="shrink-0 text-muted-foreground">{"\u00b7"}</span>
      <span className="min-w-0 truncate text-foreground/85">
        <InlineNoteText text={note.line} />
      </span>
    </span>
  );
}

function NoteHeader({ note, className }: { note: PendingHeadsUp; className?: string }) {
  return (
    <div className={className}>
      <div className="font-medium text-muted-foreground text-xs">
        {"\u2726"} {note.tag}
      </div>
      <p className="mt-1 line-clamp-4 text-foreground text-sm">
        <InlineNoteText text={note.line} />
      </p>
      {note.evidence ? (
        <p className="mt-1 line-clamp-2 text-muted-foreground text-xs">
          <InlineNoteText text={note.evidence} />
        </p>
      ) : null}
    </div>
  );
}
