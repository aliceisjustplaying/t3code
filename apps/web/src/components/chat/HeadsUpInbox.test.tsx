// @vitest-environment jsdom
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId, TurnItemId, type HeadsUpInboxEntry } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";

const state = vi.hoisted(() => ({
  view: "unresolved",
  pending: false,
  connected: true,
  deniedEnvironment: null as EnvironmentId | null,
  acknowledged: false,
  revision: 0,
  listeners: new Set<() => void>(),
  read: vi.fn(async () => ({ _tag: "Success" })),
  resolve: vi.fn(async (_input: unknown) => ({ _tag: "Success" })),
  refresh: vi.fn(),
  navigate: vi.fn(),
  draftReady: vi.fn(),
}));
vi.mock("@effect/atom-react", () => ({
  useAtomValue: () => AsyncResult.success({ phase: state.connected ? "connected" : "offline" }),
}));
vi.mock("../../state/session", () => ({
  useEnvironmentScope: (environment: EnvironmentId) => environment !== state.deniedEnvironment,
  readEnvironmentScope: (environment: EnvironmentId) => environment !== state.deniedEnvironment,
}));
vi.mock("../../connection/catalog", () => ({ environmentCatalog: { stateAtom: () => null } }));
vi.mock("../../state/headsUpInbox", () => ({
  headsUpInbox: {
    summary: () => "summary",
    page: ({ input }: { input: { view: string } }) => {
      state.view = input.view;
      return "page";
    },
    read: "read",
    resolve: "resolve",
  },
}));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: "read" | "resolve") => state[command],
}));
vi.mock("../../state/query", async () => {
  const { useSyncExternalStore } = await import("react");
  const subscribe = (listener: () => void) => {
    state.listeners.add(listener);
    return () => state.listeners.delete(listener);
  };
  return {
    useEnvironmentQuery: (query: string) => {
      // Query updates must reach memoized children, just like real atom subscriptions.
      useSyncExternalStore(subscribe, () => state.revision);
      return {
        data:
          query === "page"
            ? {
                items: [
                  {
                    ...entry,
                    readAt: state.acknowledged ? entry.createdAt : null,
                    note: {
                      ...entry.note,
                      ...(state.view === "reviewed" ? { resolution: "dismiss" } : {}),
                    },
                  },
                ],
              }
            : { unreadCount: state.acknowledged ? 0 : 3, unresolvedCount: 74, reviewedCount: 1 },
        isPending: query === "page" && state.pending,
        error: null,
        refresh: state.refresh,
      };
    },
  };
});
vi.mock("@tanstack/react-router", () => ({ useNavigate: () => state.navigate }));
vi.mock("../ChatMarkdown", () => ({ default: ({ text }: { text: string }) => <p>{text}</p> }));
import { HeadsUpInbox } from "./HeadsUpInbox";
import { useComposerDraftStore } from "../../composerDraftStore";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";

const environmentId = EnvironmentId.make("environment");
const otherEnvironmentId = EnvironmentId.make("other-environment");
const threadId = ThreadId.make("source");
const entry: HeadsUpInboxEntry = {
  id: "notice",
  threadId,
  turnItemId: TurnItemId.make("notice"),
  sourceThreadId: threadId,
  sourceThreadTitle: "Source",
  targetThreadId: threadId,
  targetThreadTitle: "Source",
  providerThreadId: null,
  createdAt: "2026-10-06T00:00:00.000Z",
  readAt: null,
  note: {
    noteId: "note",
    tag: "Heads up",
    line: "A shared cache",
    explanation: "Check the cache.",
    evidence: "cache receipt",
  },
};
let root: Root;
let container: HTMLElement;
const render = async (environment = environmentId, currentThreadId = threadId) => {
  await act(async () => {
    state.revision++;
    for (const listener of state.listeners) listener();
    root.render(
      <HeadsUpInbox
        environmentId={environment}
        threadId={currentThreadId}
        onDraftReady={state.draftReady}
      />,
    );
  });
};
const button = (text: string) => {
  const found = [...document.querySelectorAll<HTMLButtonElement>("button")].find(
    (candidate) => candidate.textContent?.trim() === text,
  );
  if (!found) throw new Error(`Missing button: ${text}`);
  return found;
};
async function openInbox(environment = environmentId, currentThreadId = threadId) {
  await render(environment, currentThreadId);
  await act(async () =>
    document.querySelector<HTMLButtonElement>('button[aria-label^="You should know"]')!.click(),
  );
}
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  state.pending = false;
  state.connected = true;
  state.deniedEnvironment = null;
  state.acknowledged = false;
  state.read.mockClear();
  state.resolve.mockReset().mockResolvedValue({ _tag: "Success" });
  state.refresh.mockClear();
  state.navigate.mockClear();
  state.draftReady.mockReset();
  useComposerDraftStore.getState().clearComposerContent(scopeThreadRef(environmentId, threadId));
  useComposerDraftStore
    .getState()
    .clearComposerContent(scopeThreadRef(otherEnvironmentId, threadId));
  container = document.createElement("div");
  document.body.appendChild(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

it.each(["dismiss", "knew"] as const)(
  "%s works from a collapsed row without expanding or sending a draft",
  async (resolution) => {
    await openInbox();
    expect(state.read).toHaveBeenCalledTimes(1);
    let finishSave!: (result: { _tag: string }) => void;
    const saving = new Promise<{ _tag: string }>((resolve) => {
      finishSave = resolve;
    });
    state.resolve.mockReturnValueOnce(saving);
    await act(async () => button(resolution === "dismiss" ? "Dismiss" : "Knew").click());
    expect(state.resolve).toHaveBeenCalledWith({
      environmentId,
      input: { threadId, turnItemId: entry.turnItemId, resolution },
    });
    expect(
      document.querySelector('button[aria-controls="ysk-notice"][aria-expanded="true"]'),
    ).toBeNull();
    expect(document.body.textContent).not.toContain("Check the cache.");
    expect(document.body.textContent).toContain("Saving…");
    expect(button("Dismiss").disabled).toBe(true);
    expect(button("Knew").disabled).toBe(true);
    await act(async () => finishSave({ _tag: "Success" }));
    expect(state.refresh).toHaveBeenCalledTimes(1);
    expect(state.read).toHaveBeenCalledTimes(1);
    expect(document.body.textContent).toContain("Available in Reviewed.");
  },
);

it("Reviewed restores inline, and failed actions stay available for retry", async () => {
  await openInbox();
  await act(async () => button("reviewed (1)").click());
  state.resolve.mockResolvedValueOnce({ _tag: "Failure" });
  await act(async () => button("Undo").click());
  expect(document.querySelector('[role="alert"]')?.textContent).toContain("could not be updated");
  expect(button("Undo").disabled).toBe(false);
  await act(async () => button("Undo").click());
  expect(state.resolve).toHaveBeenLastCalledWith({
    environmentId,
    input: { threadId, turnItemId: entry.turnItemId, resolution: null },
  });
  expect(document.body.textContent).toContain("Restored to Unresolved.");
  expect(
    document.querySelector('button[aria-controls="ysk-notice"][aria-expanded="true"]'),
  ).toBeNull();
});

it("read-triggered refreshes do not insert a banner ahead of mounted rows in either view", async () => {
  await openInbox();
  const row = document.querySelector('button[aria-controls="ysk-notice"]')!;
  const section = row.closest("section[aria-label]")!;
  expect(row.textContent).toContain("Unread");
  const contentBefore = section.parentElement!.textContent;
  state.pending = true;
  await render();
  expect(section.parentElement!.getAttribute("aria-busy")).toBe("true");
  expect(section.parentElement!.textContent).toBe(contentBefore);
  expect(section.parentElement!.firstElementChild).toBe(section);
  expect(document.querySelector('button[aria-controls="ysk-notice"]')).toBe(row);
  state.pending = false;
  await act(async () => button("reviewed (1)").click());
  const reviewed = document.querySelector('button[aria-controls="ysk-notice"]')!;
  const reviewedContent = reviewed.closest("section[aria-label]")!.parentElement!.textContent;
  state.pending = true;
  await render();
  const reviewedSection = reviewed.closest("section[aria-label]")!;
  expect(reviewedSection.parentElement!.getAttribute("aria-busy")).toBe("true");
  expect(reviewedSection.parentElement!.textContent).toBe(reviewedContent);
  expect(reviewedSection.parentElement!.firstElementChild).toBe(reviewedSection);
  expect(document.querySelector('button[aria-controls="ysk-notice"]')).toBe(reviewed);
  state.pending = false;
  state.acknowledged = true;
  await render();
  expect(document.querySelector('button[aria-controls="ysk-notice"]')).toBe(reviewed);
  expect(reviewed.textContent).not.toContain("Unread");
  expect(state.read).toHaveBeenCalledTimes(1);
});

it("expansion still exposes explanation, evidence and Ask; disconnected quick actions are disabled", async () => {
  await openInbox();
  await act(async () =>
    document.querySelector<HTMLButtonElement>('button[aria-controls="ysk-notice"]')!.click(),
  );
  expect(document.body.textContent).toContain("Check the cache.");
  expect(document.body.textContent).toContain("cache receipt");
  expect(button("Ask agent · Draft").disabled).toBe(false);
  expect(state.resolve).not.toHaveBeenCalled();
  state.connected = false;
  await render();
  expect(button("Dismiss").disabled).toBe(true);
  expect(button("Knew").disabled).toBe(true);
  expect(button("Ask agent · Draft").disabled).toBe(true);
});

it.each(["unresolved", "reviewed"] as const)(
  "Ask appends to the draft and dismisses a collapsed %s notice",
  async (view) => {
    const target = scopeThreadRef(environmentId, threadId);
    useComposerDraftStore.getState().setPrompt(target, "Existing draft");
    await openInbox();
    if (view === "reviewed") await act(async () => button("reviewed (1)").click());
    expect(document.body.textContent).not.toContain("Check the cache.");
    state.connected = false;
    await render();
    expect(button("Ask agent · Draft").disabled).toBe(true);
    state.connected = true;
    await render();
    await act(async () => button("Ask agent · Draft").click());
    const prompt = useComposerDraftStore.getState().getComposerDraft(target)?.prompt;
    expect(prompt).toContain("Existing draft\n\nHere is a note offered by a side agent:");
    expect(prompt).toContain("A shared cache");
    expect(prompt).toContain("Check the cache.");
    expect(document.querySelector('button[aria-controls="ysk-notice"]')).toBeNull();
    expect(state.resolve).toHaveBeenCalledWith({
      environmentId,
      input: { threadId, turnItemId: entry.turnItemId, resolution: "dismiss" },
    });
    expect(state.navigate).not.toHaveBeenCalled();
  },
);

it.each(["Success", "Failure"] as const)(
  "Ask completing with %s after closing writes only the captured draft on success and leaves the reopened inbox alone",
  async (resultTag) => {
    const target = scopeThreadRef(environmentId, threadId);
    const otherTarget = scopeThreadRef(otherEnvironmentId, threadId);
    const store = useComposerDraftStore.getState();
    store.setPrompt(target, "Existing draft");
    store.setPrompt(otherTarget, "Other environment draft");
    await openInbox(environmentId, ThreadId.make("current"));
    let finishSave!: (result: { _tag: string }) => void;
    const saving = new Promise<{ _tag: string }>((resolve) => {
      finishSave = resolve;
    });
    state.resolve.mockReturnValueOnce(saving);
    await act(async () => button("Ask agent · Draft").click());
    expect(state.resolve).toHaveBeenCalledWith({
      environmentId,
      input: { threadId, turnItemId: entry.turnItemId, resolution: "dismiss" },
    });
    await act(async () =>
      document.querySelector<HTMLButtonElement>('button[aria-label="Back to chat"]')!.click(),
    );
    await openInbox(otherEnvironmentId);
    store.setPrompt(target, "Edited while saving");
    await act(async () => finishSave({ _tag: resultTag }));
    const prompt = useComposerDraftStore.getState().getComposerDraft(target)?.prompt;
    if (resultTag === "Success") {
      expect(prompt).toContain("Edited while saving\n\nHere is a note offered by a side agent:");
      expect(prompt).toContain("Check the cache.");
      expect(prompt?.match(/Here is a note/g)).toHaveLength(1);
    } else {
      expect(prompt).toBe("Edited while saving");
    }
    expect(useComposerDraftStore.getState().getComposerDraft(otherTarget)?.prompt).toBe(
      "Other environment draft",
    );
    expect(state.draftReady).not.toHaveBeenCalled();
    expect(state.navigate).not.toHaveBeenCalled();
    expect(button("Ask agent · Draft").disabled).toBe(false);
    expect(document.body.textContent).not.toContain("The notice could not be updated.");
  },
);

it.each(["Success", "Failure"] as const)(
  "Ask completing with %s after same-environment thread navigation preserves the new screen",
  async (resultTag) => {
    const target = scopeThreadRef(environmentId, entry.targetThreadId);
    const currentThread = ThreadId.make("current-b");
    const currentTarget = scopeThreadRef(environmentId, currentThread);
    const store = useComposerDraftStore.getState();
    store.setPrompt(target, "Captured target draft");
    store.setPrompt(currentTarget, "Current screen draft");
    let selectedJob: string | null = "current-screen-job";
    state.draftReady.mockImplementation(() => {
      selectedJob = null;
    });
    await openInbox(environmentId, ThreadId.make("current-a"));
    let finishSave!: (result: { _tag: string }) => void;
    const saving = new Promise<{ _tag: string }>((resolve) => {
      finishSave = resolve;
    });
    state.resolve.mockReturnValueOnce(saving);
    await act(async () => button("Ask agent · Draft").click());
    expect(state.resolve).toHaveBeenCalledWith({
      environmentId,
      input: { threadId: entry.threadId, turnItemId: entry.turnItemId, resolution: "dismiss" },
    });
    // Retain the open outer inbox; only the current-thread prop changes.
    await render(environmentId, currentThread);
    store.setPrompt(target, "Edited captured target draft");
    await act(async () => finishSave({ _tag: resultTag }));
    const prompt = useComposerDraftStore.getState().getComposerDraft(target)?.prompt;
    if (resultTag === "Success") {
      expect(prompt).toContain("Edited captured target draft\n\nHere is a note");
      expect(prompt?.match(/Here is a note/g)).toHaveLength(1);
    } else {
      expect(prompt).toBe("Edited captured target draft");
    }
    expect(useComposerDraftStore.getState().getComposerDraft(currentTarget)?.prompt).toBe(
      "Current screen draft",
    );
    expect(selectedJob).toBe("current-screen-job");
    expect(state.draftReady).not.toHaveBeenCalled();
    expect(state.navigate).not.toHaveBeenCalled();
    expect(button("Ask agent · Draft").disabled).toBe(false);
    expect(document.body.textContent).not.toContain("The notice could not be updated.");
    store.clearComposerContent(currentTarget);
  },
);

it("shows only unread notes and hides the badge after acknowledgment", async () => {
  await openInbox();
  expect(
    document.querySelector<HTMLButtonElement>('button[aria-label^="You should know"]')!.textContent,
  ).toContain("3");
  state.acknowledged = true;
  await render();
  await act(async () =>
    document.querySelector<HTMLButtonElement>('button[aria-label="Back to chat"]')!.click(),
  );
  const trigger = document.querySelector<HTMLButtonElement>(
    'button[aria-label^="You should know"]',
  )!;
  expect(trigger.textContent).not.toMatch(/\d/);
  expect(trigger.getAttribute("aria-label")).not.toContain("unresolved");
  expect(trigger.getAttribute("aria-label")).toContain("0 unread");
});

it("Ask preserves the draft and keeps the notice open if dismissal fails, then retries once", async () => {
  const target = scopeThreadRef(environmentId, threadId);
  useComposerDraftStore.getState().setPrompt(target, "Existing draft");
  await openInbox();
  state.draftReady.mockImplementation(() => {
    expect(useComposerDraftStore.getState().getComposerDraft(target)?.prompt).toContain(
      "Existing draft\n\nHere is a note",
    );
  });
  let finishSave!: (result: { _tag: string }) => void;
  const saving = new Promise<{ _tag: string }>((resolve) => {
    finishSave = resolve;
  });
  state.resolve.mockReturnValueOnce(saving);
  await act(async () => button("Ask agent · Draft").click());
  expect(state.draftReady).not.toHaveBeenCalled();
  expect(useComposerDraftStore.getState().getComposerDraft(target)?.prompt).toBe("Existing draft");
  await act(async () => finishSave({ _tag: "Failure" }));
  expect(state.draftReady).not.toHaveBeenCalled();
  expect(useComposerDraftStore.getState().getComposerDraft(target)?.prompt).toBe("Existing draft");
  expect(document.body.textContent).toContain("The notice could not be updated. Try again.");
  expect(button("Ask agent · Draft").disabled).toBe(false);
  await act(async () => button("Ask agent · Draft").click());
  expect(
    useComposerDraftStore
      .getState()
      .getComposerDraft(target)
      ?.prompt?.match(/Here is a note/g),
  ).toHaveLength(1);
  expect(state.draftReady).toHaveBeenCalledTimes(1);
  expect(document.querySelector('button[aria-controls="ysk-notice"]')).toBeNull();
});

it("read-only destination stays browsable without read acknowledgments or notice mutations", async () => {
  state.deniedEnvironment = otherEnvironmentId;
  await openInbox(otherEnvironmentId);
  expect(state.read).not.toHaveBeenCalled();
  for (const label of ["Dismiss", "Knew", "Ask agent · Draft"]) {
    expect(button(label).disabled).toBe(true);
    await act(async () => button(label).click());
  }
  await act(async () => button("reviewed (1)").click());
  expect(button("Undo").disabled).toBe(true);
  await act(async () => button("Undo").click());
  expect(state.resolve).not.toHaveBeenCalled();
  expect(state.draftReady).not.toHaveBeenCalled();
  state.deniedEnvironment = null;
  await render(otherEnvironmentId);
  expect(state.read).toHaveBeenCalledWith({
    environmentId: otherEnvironmentId,
    input: { threadId, turnItemId: entry.turnItemId },
  });
  expect(button("Undo").disabled).toBe(false);
});
