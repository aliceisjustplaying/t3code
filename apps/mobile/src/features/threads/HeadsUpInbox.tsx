import { useAtomValue } from "@effect/atom-react";
import { useNavigation } from "@react-navigation/native";
import { appendHeadsUpFollowUp, groupHeadsUpInbox } from "@t3tools/client-runtime/heads-up";
import type {
  EnvironmentId,
  HeadsUpInboxEntry,
  HeadsUpInboxInput,
  ThreadId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { useEffect, useRef, useState } from "react";
import { Modal, Pressable, ScrollView, View } from "react-native";
import { SafeAreaProvider, SafeAreaView } from "react-native-safe-area-context";

import { MarkdownContent } from "../../components/MarkdownContent";
import { AppText as Text } from "../../components/AppText";
import { SymbolView } from "../../components/AppSymbol";
import { environmentCatalog } from "../../connection/catalog";
import { scopedThreadKey } from "../../lib/scopedEntities";
import { headsUpInbox } from "../../state/headsUpInbox";
import { useEnvironmentQuery } from "../../state/query";
import { useAtomCommand } from "../../state/use-atom-command";
import {
  getComposerDraftSnapshot,
  setComposerDraftText,
  waitForComposerDraftsLoaded,
} from "../../state/use-composer-drafts";
import { RequestActionButton } from "./RequestActionButton";

export function useHeadsUpInbox(
  environmentId: EnvironmentId,
  threadId: ThreadId,
  onDraftReady: () => void,
) {
  const [open, setOpen] = useState(false);
  const summary = useEnvironmentQuery(headsUpInbox.summary({ environmentId, input: {} }));
  const connection = useAtomValue(environmentCatalog.stateAtom(environmentId));
  const connected = Option.getOrNull(AsyncResult.value(connection))?.phase === "connected";
  const count = summary.data?.unresolvedCount;
  const unreadCount = summary.data?.unreadCount;
  const highlight = (unreadCount ?? 0) > 0;
  const button = (
    <Pressable
      accessibilityRole="button"
      accessibilityLabel={`You should know${count === undefined ? ", loading" : `, ${count} unresolved, ${unreadCount ?? 0} unread`}${connected ? "" : ", disconnected"}`}
      accessibilityState={{ expanded: open }}
      onPress={() => setOpen(true)}
      className={`min-h-11 min-w-11 flex-row items-center justify-center gap-1 rounded-xl px-2 ${highlight ? "bg-subtle-strong" : "bg-transparent"}`}
    >
      <SymbolView name="lightbulb" size={20} tintColorClassName="accent-header-foreground" />
      <Text className="text-xs font-t3-bold text-foreground-muted">
        {connected && !summary.error ? (count ?? "…") : "!"}
      </Text>
    </Pressable>
  );
  return {
    button,
    revision: `${environmentId}:${count}:${connected}:${highlight}:${open}`,
    modal: (
      <Modal
        visible={open}
        presentationStyle="fullScreen"
        animationType="slide"
        onRequestClose={() => setOpen(false)}
      >
        {open ? (
          <View className="flex-1 bg-screen">
            <SafeAreaProvider style={{ flex: 1 }}>
              <SafeAreaView style={{ flex: 1 }}>
                <View className="flex-row items-center gap-3 border-b border-border px-3 py-2">
                  <Pressable
                    accessibilityRole="button"
                    accessibilityLabel="Back to chat"
                    onPress={() => setOpen(false)}
                    className="min-h-11 flex-row items-center gap-1 px-2"
                  >
                    <SymbolView
                      name="chevron.left"
                      size={18}
                      tintColorClassName="accent-foreground"
                    />
                    <Text className="text-base text-foreground">Back</Text>
                  </Pressable>
                  <Text accessibilityRole="header" className="text-lg font-t3-bold text-foreground">
                    You should know
                  </Text>
                </View>
                <Text className="px-4 py-1 text-xs text-foreground-muted">
                  Current environment · All projects and threads
                </Text>
                <InboxPages
                  key={`${environmentId}:${threadId}`}
                  environmentId={environmentId}
                  threadId={threadId}
                  connected={connected}
                  onClose={() => setOpen(false)}
                  onDraftReady={onDraftReady}
                />
                <Text className="border-t border-border px-4 py-3 text-xs text-foreground-muted">
                  Opening marks notices read, not resolved.
                </Text>
              </SafeAreaView>
            </SafeAreaProvider>
          </View>
        ) : null}
      </Modal>
    ),
  };
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
  readonly connected: boolean;
  readonly onClose: () => void;
  readonly onDraftReady: () => void;
}) {
  const [view, setView] = useState<HeadsUpInboxInput["view"]>("unresolved");
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
  const page = useEnvironmentQuery(headsUpInbox.page({ environmentId, input: { view } }));
  const read = useAtomCommand(headsUpInbox.read, { reportFailure: false });
  const resolve = useAtomCommand(headsUpInbox.resolve, { reportFailure: false });
  const navigation = useNavigation();
  const items = page.data?.items;
  const groups = groupHeadsUpInbox(items ?? []);
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
  const changeView = (nextView: HeadsUpInboxInput["view"]) => {
    setView(nextView);
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
      await waitForComposerDraftsLoaded();
      if (!mounted.current) return;
      const result = await resolve({
        environmentId,
        input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution: "dismiss" },
      });
      if (result._tag !== "Success") {
        if (mounted.current) setActionError("The notice could not be updated. Try again.");
        return;
      }
      const draftKey = scopedThreadKey(environmentId, entry.targetThreadId);
      setComposerDraftText(
        draftKey,
        appendHeadsUpFollowUp(getComposerDraftSnapshot(draftKey).text, entry),
      );
      // Closing the inbox must not discard a draft for an already-dismissed notice.
      if (!mounted.current) return;
      onDraftReady();
      onClose();
      if (threadId !== entry.targetThreadId)
        navigation.navigate("Thread", { environmentId, threadId: entry.targetThreadId });
    } catch {
      if (mounted.current) setActionError("The draft could not be updated. Try again.");
    } finally {
      if (mounted.current) setBusy(null);
    }
  };
  const loadError = page.error ?? summary.error;
  return (
    <>
      <View className="flex-row gap-1 border-b border-border px-4">
        {(["unresolved", "reviewed"] as const).map((tab) => (
          <Pressable
            key={tab}
            accessibilityRole="button"
            accessibilityState={{ selected: view === tab }}
            onPress={() => changeView(tab)}
            className={`min-h-11 justify-center px-3 ${view === tab ? "border-b-2 border-primary" : ""}`}
          >
            <Text className="text-sm capitalize text-foreground">
              {tab}{" "}
              {summary.data
                ? `(${tab === "unresolved" ? summary.data.unresolvedCount : summary.data.reviewedCount})`
                : ""}
            </Text>
          </Pressable>
        ))}
      </View>
      <ScrollView
        key={view}
        className="flex-1"
        contentContainerClassName="pb-4"
        accessibilityState={{ busy: page.isPending }}
      >
        {!connected ? (
          <Text accessibilityLiveRegion="polite" className="p-4 text-sm text-foreground-muted">
            Disconnected. Reconnect to refresh or update notices.
          </Text>
        ) : null}
        {loadError ? (
          <View className="gap-2 p-4">
            <Text accessibilityRole="alert" className="text-sm text-danger">
              {loadError}
            </Text>
            <RequestActionButton
              label="Retry"
              tone="secondary"
              onPress={() => {
                page.refresh();
                summary.refresh();
              }}
            />
          </View>
        ) : null}
        {/* Read acknowledgments revalidate this page; keep loaded rows in place. */}
        {!page.data && !loadError ? (
          <Text accessibilityLiveRegion="polite" className="p-4 text-sm text-foreground-muted">
            Loading notices…
          </Text>
        ) : null}
        {connected && !loadError && page.data?.items.length === 0 ? (
          <Text className="p-6 text-sm text-foreground-muted">
            {view === "reviewed"
              ? "No reviewed notices yet."
              : "Nothing unresolved. Reviewed notices can be restored anytime."}
          </Text>
        ) : null}
        {groups.map((group) => (
          <View key={group.key}>
            <Text
              accessibilityRole="header"
              className="border-b border-border px-4 py-2 text-sm font-t3-bold text-foreground"
            >
              {group.title}
            </Text>
            {group.items.map((entry) => (
              <View key={entry.id} className="border-b border-border">
                <Pressable
                  accessibilityRole="button"
                  accessibilityState={{ expanded: expanded === entry.id }}
                  onPress={() => setExpanded(expanded === entry.id ? null : entry.id)}
                  className={`gap-1 px-4 py-2 ${expanded === entry.id ? "bg-subtle" : ""}`}
                >
                  <View className="flex-row justify-between gap-3">
                    <Text className="text-xs text-foreground-muted">
                      {entry.note.tag}
                      {entry.readAt === null ? " · Unread" : ""}
                    </Text>
                    <Text className="shrink text-xs text-foreground-muted">
                      {new Date(entry.createdAt).toLocaleString()}
                    </Text>
                  </View>
                  <Text
                    numberOfLines={expanded === entry.id ? undefined : 2}
                    className="text-sm font-t3-bold text-foreground"
                  >
                    {entry.note.line}
                  </Text>
                </Pressable>
                <View className="flex-row flex-wrap items-center gap-1 px-4 pb-2">
                  <View className="min-w-0 flex-1">
                    <Text
                      numberOfLines={expanded === entry.id ? undefined : 1}
                      className="text-xs text-foreground-muted"
                    >
                      From {entry.sourceThreadTitle}
                    </Text>
                    {entry.note.resolution ? (
                      <Text className="text-xs text-foreground-muted">
                        {entry.note.resolution === "knew"
                          ? "Knew"
                          : entry.note.resolution === "dismiss"
                            ? "Dismissed"
                            : "Reviewed"}
                      </Text>
                    ) : null}
                  </View>
                  <View className="w-full flex-row flex-wrap gap-2">
                    <RequestActionButton
                      label="Ask agent · Draft"
                      accessibilityLabel={`Ask agent · Draft: ${entry.note.line}`}
                      disabled={!connected || busy !== null}
                      onPress={(event) => {
                        event.stopPropagation();
                        void ask(entry);
                      }}
                    />
                    {view === "reviewed" ? (
                      <RequestActionButton
                        label={
                          entry.note.resolution === "dismiss" || entry.note.resolution === "knew"
                            ? "Undo"
                            : "Restore"
                        }
                        accessibilityLabel={`${entry.note.resolution === "dismiss" || entry.note.resolution === "knew" ? "Undo" : "Restore"} · Restore to unresolved: ${entry.note.line}`}
                        tone="secondary"
                        disabled={!connected || busy !== null}
                        onPress={(event) => {
                          event.stopPropagation();
                          void act(entry, null);
                        }}
                      />
                    ) : (
                      <>
                        <RequestActionButton
                          label="Dismiss"
                          tone="secondary"
                          disabled={!connected || busy !== null}
                          accessibilityLabel={`Dismiss: ${entry.note.line}`}
                          onPress={(event) => {
                            event.stopPropagation();
                            void act(entry, "dismiss");
                          }}
                        />
                        <RequestActionButton
                          label="Knew"
                          tone="secondary"
                          disabled={!connected || busy !== null}
                          accessibilityLabel={`Knew: ${entry.note.line}`}
                          onPress={(event) => {
                            event.stopPropagation();
                            void act(entry, "knew");
                          }}
                        />
                      </>
                    )}
                  </View>
                  {busy === entry.id ? (
                    <Text
                      accessibilityLiveRegion="polite"
                      className="text-xs text-foreground-muted"
                    >
                      Saving…
                    </Text>
                  ) : null}
                </View>
                {expanded === entry.id ? (
                  <View className="gap-2 bg-subtle px-4 py-2">
                    <MarkdownContent
                      markdown={entry.note.explanation ?? "No additional explanation was supplied."}
                    />
                    <Text className="text-xs text-foreground-muted">
                      Source: {entry.sourceThreadTitle}
                      {"\n"}Draft target: {entry.targetThreadTitle}
                    </Text>
                    {entry.note.evidence ? (
                      <>
                        <Text className="text-xs font-t3-bold text-foreground">Evidence</Text>
                        <Text
                          selectable
                          className="rounded-lg border border-border p-3 font-mono text-xs text-foreground"
                        >
                          {entry.note.evidence}
                        </Text>
                      </>
                    ) : null}
                    <Text className="text-xs text-foreground-muted">
                      Ask agent adds the note to the target thread’s unsent draft and dismisses it
                      from Unresolved. Nothing is sent.
                    </Text>
                  </View>
                ) : null}
              </View>
            ))}
          </View>
        ))}
      </ScrollView>
      {readError ? (
        <View className="gap-2 px-4 py-2">
          <Text accessibilityRole="alert" className="text-xs text-danger">
            Some notices could not be marked read.
          </Text>
          <RequestActionButton
            label="Retry marking read"
            tone="secondary"
            onPress={() => {
              attemptedReads.current.clear();
              setReadError(false);
              page.refresh();
            }}
          />
        </View>
      ) : null}
      {actionError ? (
        <Text accessibilityRole="alert" className="px-4 py-2 text-sm text-danger">
          {actionError}
        </Text>
      ) : null}
      {feedback ? (
        <Text accessibilityLiveRegion="polite" className="px-4 py-2 text-xs text-foreground-muted">
          {feedback}
        </Text>
      ) : null}
    </>
  );
}
