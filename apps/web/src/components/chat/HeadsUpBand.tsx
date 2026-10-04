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
import { ChevronDownIcon } from "lucide-react";
import { useCallback, useEffect, useMemo, useState, type KeyboardEvent } from "react";

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
import { ComposerBanner } from "./ComposerBanner";
import type { ComposerBannerStackItem } from "./ComposerBannerStack";

/**
 * Heads-up notes ("you should know") pinned above the composer as one band. The
 * oldest open note leads it; "N more" expands the rest below it as a compact
 * list with the same answers. A note stays until the user answers it here, and
 * every answer but "Ask agent" can be undone. Returns the composer's banner
 * items with the band added, and the explanation dialog to mount.
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
  const [listOpen, setListOpen] = useState(false);

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

  useEffect(() => {
    const onKey = (event: globalThis.KeyboardEvent) => {
      if (
        !top ||
        explained ||
        event.defaultPrevented ||
        event.repeat ||
        event.metaKey ||
        event.ctrlKey ||
        event.altKey
      )
        return;
      const target = event.target;
      if (
        target instanceof Element &&
        target.closest("input, textarea, [contenteditable=true], [role=dialog]")
      )
        return;
      const action = ({ e: "learn", k: "knew", a: "send", x: "dismiss" } as const)[
        event.key as "e" | "k" | "a" | "x"
      ];
      if (!action || (action === "learn" && !top.explanation)) return;
      event.preventDefault();
      act(top, action);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [act, explained, top]);

  const bannerItems = useMemo<ReadonlyArray<ComposerBannerStackItem>>(() => {
    if (top === null) return input.bannerItems;
    const rest = notes.slice(1);
    const expanded = listOpen && rest.length > 0;
    const item: ComposerBannerStackItem = {
      id: "heads-up",
      variant: "info",
      icon: <span className="text-primary">{"\u2726"}</span>,
      title: top.tag,
      description: <InlineNoteText text={top.line} />,
      actions: (
        <>
          {rest.length > 0 ? (
            <Button
              size="xs"
              variant="ghost-muted"
              aria-expanded={expanded}
              aria-label={`${rest.length} more heads-up${rest.length === 1 ? "" : "s"}`}
              onClick={() => setListOpen(!expanded)}
            >
              {rest.length}
              <span className="@max-[400px]:hidden">more</span>
              <ChevronDownIcon className={cn("size-3.5", !expanded && "rotate-180")} />
            </Button>
          ) : null}
          <NoteActions note={top} onAct={act} />
        </>
      ),
      dismissLabel: "Dismiss heads-up",
      onDismiss: () => act(top, "dismiss"),
      ...(expanded
        ? {
            children: (
              <ComposerBanner.Scroll className="max-h-[min(12rem,30dvh)]">
                <ComposerBanner.Children render={<ul role="list" />} aria-label="More heads-ups">
                  {rest.map((note) => (
                    <ComposerBanner.Row key={note.turnItemId} render={<li />} layout="wrap-actions">
                      <ComposerBanner.Icon />
                      {/* Wraps instead of truncating: the list is where a long note is read. */}
                      <ComposerBanner.Content className="block py-1 wrap-anywhere text-muted-foreground">
                        {note.tag === top.tag ? null : (
                          <span className="me-1 font-medium text-foreground">{note.tag}</span>
                        )}
                        <InlineNoteText text={note.line} />
                      </ComposerBanner.Content>
                      <ComposerBanner.Actions>
                        <NoteActions note={note} onAct={act} />
                        <ComposerBanner.Dismiss
                          aria-label="Dismiss heads-up"
                          onClick={() => act(note, "dismiss")}
                        />
                      </ComposerBanner.Actions>
                    </ComposerBanner.Row>
                  ))}
                </ComposerBanner.Children>
              </ComposerBanner.Scroll>
            ),
          }
        : {}),
    };
    return [item, ...input.bannerItems];
  }, [act, input.bannerItems, listOpen, notes, top]);

  const closeExplanation = () => setExplained(null);
  const onExplanationKey = (event: KeyboardEvent) => {
    if (explained === null || event.key !== "a" || event.metaKey || event.ctrlKey) return;
    event.preventDefault();
    closeExplanation();
    askAgent(explained);
  };

  const overlay = (
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
  );

  return { bannerItems, overlay };
}

/** A note's answers besides Dismiss, which is the row's close button; mirrors Pi's /ysk menu. */
function NoteActions({
  note,
  onAct,
}: {
  note: PendingHeadsUp;
  onAct: (note: PendingHeadsUp, action: OrchestrationV2HeadsUpAction) => void;
}) {
  const actions: Array<readonly [OrchestrationV2HeadsUpAction, string]> = [];
  if (note.explanation) actions.push(["learn", "Explain"]);
  actions.push(["knew", "Knew"], ["send", "Ask agent"]);
  return actions.map(([action, label]) => (
    <Button key={action} size="xs" variant="ghost" onClick={() => onAct(note, action)}>
      {label}
    </Button>
  ));
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
