import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import {
  ComposerContextId,
  EnvironmentId,
  ThreadId,
  TurnItemId,
  type HeadsUpInboxEntry,
} from "@t3tools/contracts";
import { act, createElement, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { AsyncResult } from "effect/reactivity";

const ui = vi.hoisted(() => ({
  presses: new Map<string, () => void>(),
  actions: new Map<
    string,
    { press: (event: { stopPropagation: () => void }) => void; disabled: boolean }
  >(),
  view: "unresolved",
  ask: null as null | (() => void),
  askDisabled: false,
  visible: false,
  errors: [] as ReactNode[],
  navigate: vi.fn(),
  draftReady: vi.fn(),
  threadId: "current",
  environmentId: "environment",
  command: vi.fn(async (_input: unknown) => ({ _tag: "Success" })),
}));
const storage = vi.hoisted(() => ({
  document: "",
  barrier: Promise.resolve(),
  error: null as Error | null,
  Directory: class {
    create() {}
    list() {
      return [];
    }
  },
  File: class {
    exists = true;
    create() {}
    moveSync() {}
    async text() {
      await storage.barrier;
      if (storage.error) throw storage.error;
      return storage.document;
    }
    write(value: string) {
      storage.document = value;
    }
  },
}));
vi.mock("expo-file-system", () => ({
  Directory: storage.Directory,
  File: storage.File,
  Paths: { document: { uri: "file:///documents" } },
}));
vi.mock("../../lib/uuid", () => ({ uuidv4: () => "uuid", randomHex: () => "0000" }));
vi.mock("../../state/assets", () => ({ assetEnvironment: {} }));
vi.mock("../../state/attachments", () => ({ attachmentEnvironment: {} }));
vi.mock("../../state/session", () => ({ environmentSession: {} }));
vi.mock("../../features/sharing/incoming-share-storage", () => ({
  loadIncomingShareDrafts: async () => [],
}));
vi.mock("@t3tools/client-runtime/state/runtime", () => ({
  createEnvironmentRpcCommand: () => Symbol("command"),
}));
vi.mock("@effect/atom-react", () => ({
  useAtomValue: () => AsyncResult.success({ phase: "connected" }),
}));
vi.mock("@react-navigation/native", () => ({ useNavigation: () => ({ navigate: ui.navigate }) }));
vi.mock("../../connection/catalog", () => ({ environmentCatalog: { stateAtom: () => null } }));
vi.mock("../../state/headsUpInbox", () => ({
  headsUpInbox: {
    summary: () => "summary",
    page: ({ input }: { input: { view: string } }) => {
      ui.view = input.view;
      return "page";
    },
    read: "read",
    resolve: "resolve",
  },
}));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => ui.command }));
vi.mock("../../state/query", () => ({
  useEnvironmentQuery: (query: string) => ({
    data:
      query === "page"
        ? {
            items: [
              {
                ...entry,
                note: {
                  ...entry.note,
                  ...(ui.view === "reviewed" ? { resolution: "dismiss" } : {}),
                },
              },
            ],
          }
        : { unreadCount: 0, unresolvedCount: 1, reviewedCount: 0 },
    isPending: false,
    error: null,
    refresh: () => {},
  }),
}));
vi.mock("react-native", () => ({
  Modal: ({ visible, children }: { visible: boolean; children: ReactNode }) => {
    ui.visible = visible;
    return visible ? children : null;
  },
  Pressable: ({
    accessibilityLabel,
    accessibilityState,
    onPress,
    children,
  }: {
    accessibilityLabel?: string;
    accessibilityState?: { expanded?: boolean };
    onPress: () => void;
    children: ReactNode;
  }) => {
    ui.presses.set(
      accessibilityLabel ?? (accessibilityState?.expanded !== undefined ? "notice" : "tab"),
      onPress,
    );
    return children;
  },
  View: ({ children }: { children: ReactNode }) => children,
  ScrollView: ({ children }: { children: ReactNode }) => children,
}));
vi.mock("react-native-safe-area-context", () => ({
  SafeAreaView: ({ children }: { children: ReactNode }) => children,
  SafeAreaProvider: ({ children }: { children: ReactNode }) => children,
}));
vi.mock("../../components/MarkdownContent", () => ({ MarkdownContent: () => null }));
vi.mock("../../components/AppSymbol", () => ({ SymbolView: () => null }));
vi.mock("../../components/AppText", () => ({
  AppText: ({
    accessibilityRole,
    children,
  }: {
    accessibilityRole?: string;
    children: ReactNode;
  }) => {
    if (accessibilityRole === "alert") ui.errors.push(children);
    return null;
  },
}));
vi.mock("./RequestActionButton", () => ({
  RequestActionButton: ({
    label,
    onPress,
    disabled,
  }: {
    label: string;
    onPress: (event: { stopPropagation: () => void }) => void;
    disabled: boolean;
  }) => {
    ui.actions.set(label, { press: onPress, disabled });
    if (label === "Ask agent · Draft") {
      ui.ask = () => onPress({ stopPropagation() {} });
      ui.askDisabled = disabled;
    }
    return null;
  },
}));

import { appAtomRegistry } from "../../state/atom-registry";
import {
  composerDraftsAtom,
  getComposerDraftSnapshot,
  resetComposerDraftsLoadState,
  setComposerDraftText,
  waitForComposerDraftsLoaded,
} from "../../state/use-composer-drafts";
import { useHeadsUpInbox } from "./HeadsUpInbox";

const environmentId = EnvironmentId.make("environment");
const targetThread = ThreadId.make("target");
const key = "environment:target";
const entry: HeadsUpInboxEntry = {
  id: "notice",
  threadId: ThreadId.make("source"),
  turnItemId: TurnItemId.make("notice"),
  sourceThreadId: ThreadId.make("source"),
  sourceThreadTitle: "Source",
  targetThreadId: targetThread,
  targetThreadTitle: "Target",
  providerThreadId: null,
  createdAt: "2026-10-06T00:00:00.000Z",
  readAt: "2026-10-06T00:00:00.000Z",
  note: {
    noteId: "note",
    tag: "Heads up",
    line: "A shared cache",
    explanation: "Check the cache.",
  },
};
const draft = {
  text: "Existing work [Skill](t3-context://v1/skill/skill)",
  attachments: [
    {
      id: "image",
      type: "image" as const,
      name: "image.png",
      mimeType: "image/png",
      sizeBytes: 42,
      fileUri: "file:///documents/image.png",
      previewUri: "file:///documents/image.png",
    },
  ],
  context: {
    version: 1 as const,
    records: [
      {
        version: 1 as const,
        contextId: ComposerContextId.make("skill"),
        kind: "skill" as const,
        label: "Skill",
        name: "skill",
      },
    ],
  },
};
let root: Root;
function Probe() {
  const inbox = useHeadsUpInbox(
    EnvironmentId.make(ui.environmentId),
    ThreadId.make(ui.threadId),
    ui.draftReady,
  );
  return (
    <>
      {inbox.button}
      {inbox.modal}
    </>
  );
}
async function openNotice() {
  await act(async () => root.render(createElement(Probe)));
  await act(async () => ui.presses.get("You should know, 0 unread")!());
  ui.command.mockClear();
}
beforeEach(() => {
  vi.useFakeTimers();
  resetComposerDraftsLoadState();
  appAtomRegistry.set(composerDraftsAtom, {});
  storage.document = JSON.stringify({ schemaVersion: 1, drafts: { [key]: draft } });
  storage.barrier = Promise.resolve();
  storage.error = null;
  ui.presses.clear();
  ui.actions.clear();
  ui.ask = null;
  ui.errors = [];
  ui.navigate.mockClear();
  ui.draftReady.mockReset();
  ui.threadId = "current";
  ui.environmentId = environmentId;
  ui.command.mockReset().mockResolvedValue({ _tag: "Success" });
  const document = { nodeType: 9, addEventListener() {}, removeEventListener() {} };
  const container = {
    nodeType: 1,
    tagName: "DIV",
    namespaceURI: "http://www.w3.org/1999/xhtml",
    ownerDocument: document,
    addEventListener() {},
    removeEventListener() {},
  };
  vi.stubGlobal("document", document);
  vi.stubGlobal("window", { document, HTMLIFrameElement: EventTarget });
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  root = createRoot(container as unknown as HTMLElement);
});
afterEach(async () => {
  await act(async () => root.unmount());
  resetComposerDraftsLoadState();
  appAtomRegistry.set(composerDraftsAtom, {});
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("Collapsed Ask waits for disk hydration and appends to the latest draft without losing attachments or context", async () => {
  const hydration = Promise.withResolvers<void>();
  storage.barrier = hydration.promise;
  await openNotice();
  await act(async () => {
    ui.ask!();
  });
  const visibleWhileLoading = ui.visible;
  const disabledWhileLoading = ui.askDisabled;
  await act(async () => {
    hydration.resolve();
    await waitForComposerDraftsLoaded();
  });
  expect(getComposerDraftSnapshot(key)).toMatchObject({
    ...draft,
    text: expect.stringContaining(`${draft.text}\n\nHere is a note offered by a side agent:`),
  });
  expect(visibleWhileLoading).toBe(true);
  expect(disabledWhileLoading).toBe(true);
  expect(ui.visible).toBe(false);
  expect(ui.navigate).toHaveBeenCalledWith("Thread", { environmentId, threadId: targetThread });
  expect(ui.command).toHaveBeenCalledWith({
    environmentId,
    input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution: "dismiss" },
  });
});

it.each(["Success", "Failure"] as const)(
  "Ask completing with %s after closing writes only the captured hydrated draft on success and leaves the reopened inbox alone",
  async (resultTag) => {
    await act(async () => waitForComposerDraftsLoaded());
    await openNotice();
    const saving = Promise.withResolvers<{ _tag: string }>();
    ui.command.mockReturnValueOnce(saving.promise);
    await act(async () => {
      ui.ask!();
    });
    expect(ui.command).toHaveBeenCalledWith({
      environmentId,
      input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution: "dismiss" },
    });
    await act(async () => ui.presses.get("Back to chat")!());
    ui.environmentId = "other-environment";
    await openNotice();
    setComposerDraftText(key, `${draft.text}\nEdited while saving`);
    const otherKey = "other-environment:target";
    setComposerDraftText(otherKey, "Other environment draft");
    await act(async () => saving.resolve({ _tag: resultTag }));
    const capturedDraft = getComposerDraftSnapshot(key);
    expect(capturedDraft.attachments).toEqual(draft.attachments);
    expect(capturedDraft.context).toEqual(draft.context);
    if (resultTag === "Success") {
      expect(capturedDraft.text).toContain(
        `${draft.text}\nEdited while saving\n\nHere is a note offered by a side agent:`,
      );
      expect(capturedDraft.text).toContain("Check the cache.");
      expect(capturedDraft.text.match(/Here is a note/g)).toHaveLength(1);
    } else {
      expect(capturedDraft.text).toBe(`${draft.text}\nEdited while saving`);
    }
    expect(getComposerDraftSnapshot(otherKey).text).toBe("Other environment draft");
    expect(ui.visible).toBe(true);
    expect(ui.askDisabled).toBe(false);
    expect(ui.navigate).not.toHaveBeenCalled();
    expect(ui.draftReady).not.toHaveBeenCalled();
    expect(ui.errors).toEqual([]);
  },
);

it.each(["Success", "Failure"] as const)(
  "Ask completing with %s after same-environment thread navigation preserves the new screen",
  async (resultTag) => {
    await act(async () => waitForComposerDraftsLoaded());
    await openNotice();
    const saving = Promise.withResolvers<{ _tag: string }>();
    ui.command.mockReturnValueOnce(saving.promise);
    await act(async () => ui.ask!());
    expect(ui.command).toHaveBeenCalledWith({
      environmentId,
      input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution: "dismiss" },
    });
    let selectedJob: string | null = "current-screen-job";
    ui.draftReady.mockImplementation(() => {
      selectedJob = null;
    });
    ui.threadId = "current-b";
    // Re-render the retained hook without closing its modal first.
    await act(async () => root.render(createElement(Probe)));
    expect(ui.visible).toBe(true);
    const currentKey = "environment:current-b";
    setComposerDraftText(currentKey, "Current screen draft");
    setComposerDraftText(key, `${draft.text}\nEdited while saving`);
    await act(async () => saving.resolve({ _tag: resultTag }));
    const capturedDraft = getComposerDraftSnapshot(key);
    expect(capturedDraft.attachments).toEqual(draft.attachments);
    expect(capturedDraft.context).toEqual(draft.context);
    if (resultTag === "Success") {
      expect(capturedDraft.text).toContain(`${draft.text}\nEdited while saving\n\nHere is a note`);
      expect(capturedDraft.text.match(/Here is a note/g)).toHaveLength(1);
    } else {
      expect(capturedDraft.text).toBe(`${draft.text}\nEdited while saving`);
    }
    expect(getComposerDraftSnapshot(currentKey).text).toBe("Current screen draft");
    expect(selectedJob).toBe("current-screen-job");
    expect(ui.visible).toBe(true);
    expect(ui.askDisabled).toBe(false);
    expect(ui.navigate).not.toHaveBeenCalled();
    expect(ui.draftReady).not.toHaveBeenCalled();
    expect(ui.errors).toEqual([]);
  },
);

it("Ask preserves edits after hydration and keeps the inbox open when hydration fails", async () => {
  const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
  try {
    storage.error = new Error("Disk read failed");
    await openNotice();
    await act(async () => {
      await ui.ask!();
    });
    expect(ui.visible).toBe(true);
    expect(ui.errors).toContain("The draft could not be updated. Try again.");
    expect(ui.navigate).not.toHaveBeenCalled();
    expect(getComposerDraftSnapshot(key).text).toBe("");
    expect(ui.command).not.toHaveBeenCalled();
    storage.error = null;
    await waitForComposerDraftsLoaded();
    setComposerDraftText(key, `${draft.text}\nNew edit`);
    await act(async () => {
      await ui.ask!();
    });
    expect(getComposerDraftSnapshot(key).text).toContain(`${draft.text}\nNew edit\n\n`);
    expect(getComposerDraftSnapshot(key).attachments).toEqual(draft.attachments);
    expect(getComposerDraftSnapshot(key).context).toEqual(draft.context);
    expect(ui.visible).toBe(false);
    expect(ui.command).toHaveBeenCalledWith({
      environmentId,
      input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution: "dismiss" },
    });
  } finally {
    warning.mockRestore();
  }
});

it.each(["dismiss", "knew"] as const)(
  "%s can be saved from a collapsed native row and blocks other actions while saving",
  async (resolution) => {
    await act(async () => root.render(createElement(Probe)));
    await act(async () => ui.presses.get("You should know, 0 unread")!());
    const saving = Promise.withResolvers<{ _tag: string }>();
    ui.command.mockReturnValueOnce(saving.promise);
    const stopPropagation = vi.fn();
    await act(async () =>
      ui.actions.get(resolution === "dismiss" ? "Dismiss" : "Knew")!.press({ stopPropagation }),
    );
    expect(ui.command).toHaveBeenCalledWith({
      environmentId,
      input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution },
    });
    expect(ui.actions.get("Dismiss")!.disabled).toBe(true);
    expect(ui.actions.get("Knew")!.disabled).toBe(true);
    expect(ui.askDisabled).toBe(true);
    expect(stopPropagation).toHaveBeenCalledTimes(1);
    await act(async () => saving.resolve({ _tag: "Success" }));
    expect(ui.actions.get("Dismiss")!.disabled).toBe(false);
    expect(ui.navigate).not.toHaveBeenCalled();
  },
);

it("native Reviewed restores without expansion and exposes failed updates for retry", async () => {
  await act(async () => root.render(createElement(Probe)));
  await act(async () => ui.presses.get("You should know, 0 unread")!());
  await act(async () => ui.presses.get("tab")!());
  ui.command.mockResolvedValueOnce({ _tag: "Failure" });
  await act(async () => ui.actions.get("Undo")!.press({ stopPropagation() {} }));
  expect(ui.errors).toContain("The notice could not be updated. Try again.");
  expect(ui.actions.get("Undo")!.disabled).toBe(false);
  await act(async () => ui.actions.get("Undo")!.press({ stopPropagation() {} }));
  expect(ui.command).toHaveBeenLastCalledWith({
    environmentId,
    input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution: null },
  });
  expect(ui.ask).not.toBeNull();
  expect(ui.navigate).not.toHaveBeenCalled();
});

it("same-thread Ask reveals the prepared draft only after dismissal succeeds, preserving the view on failure", async () => {
  ui.threadId = targetThread;
  ui.draftReady.mockImplementation(() => {
    expect(getComposerDraftSnapshot(key)).toMatchObject({
      ...draft,
      text: expect.stringContaining(draft.text + "\n\nHere is a note"),
    });
  });
  await openNotice();
  const saving = Promise.withResolvers<{ _tag: string }>();
  ui.command.mockReturnValueOnce(saving.promise);
  await act(async () => {
    ui.ask!();
  });
  expect(ui.draftReady).not.toHaveBeenCalled();
  expect(ui.visible).toBe(true);
  expect(getComposerDraftSnapshot(key)).toMatchObject(draft);
  await act(async () => saving.resolve({ _tag: "Failure" }));
  expect(ui.draftReady).not.toHaveBeenCalled();
  expect(ui.visible).toBe(true);
  expect(getComposerDraftSnapshot(key)).toMatchObject(draft);
  await act(async () => {
    ui.ask!();
  });
  expect(ui.draftReady).toHaveBeenCalledTimes(1);
  expect(ui.visible).toBe(false);
  expect(ui.navigate).not.toHaveBeenCalled();
});
