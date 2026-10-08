// @vitest-environment jsdom
import { RegistryContext } from "@effect/atom-react";
import { threadJobs, type ThreadJob } from "@t3tools/client-runtime/jobs";
import {
  EnvironmentId,
  ProviderSessionId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2GetTurnItemInput,
  type OrchestrationV2GetTurnItemResult,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { act, createElement, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { AlertButton } from "react-native";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

type DetailTarget = { environmentId: EnvironmentId; input: OrchestrationV2GetTurnItemInput };
type DetailResult = AsyncResult.AsyncResult<OrchestrationV2GetTurnItemResult, Error>;
const ui = vi.hoisted(() => ({
  environmentId: "environment",
  threadId: "thread",
  jobs: [] as ThreadJob[],
  canOperate: true,
  grantAtDispatch: true,
  stop: vi.fn(async (_input: unknown) => ({ _tag: "Success" })),
  alert: vi.fn((_title: string, _message?: string, _buttons?: AlertButton[]) => {}),
  queries: new Map<
    string,
    { query: Atom.Atom<DetailResult>; response: Atom.Writable<DetailResult> }
  >(),
  requests: [] as DetailTarget[],
}));

vi.mock("react-native", () => ({
  Platform: { OS: "android" },
  Alert: { alert: ui.alert },
  ScrollView: ({ children }: { children: ReactNode }) => children,
  View: ({ children }: { children: ReactNode }) => children,
  Pressable: ({
    children,
    onPress,
    disabled,
  }: {
    children: ReactNode;
    onPress: () => void;
    disabled?: boolean;
  }) => createElement("button", { onClick: onPress, disabled }, children),
}));
vi.mock("../../components/AppText", () => ({
  AppText: ({ children }: { children: ReactNode }) => children,
}));
vi.mock("@react-navigation/native", () => ({
  StackActions: {},
  useFocusEffect: () => {},
  useNavigation: () => ({}),
}));
vi.mock("react-native-safe-area-context", () => ({ useSafeAreaInsets: () => ({ top: 0 }) }));
vi.mock("../../components/ScreenHeader", () => ({
  ScreenHeader: ({ menus = [] }: { menus?: { items: MenuItem[] }[] }) => {
    const render = (items: MenuItem[]): ReactNode =>
      items.map((item) =>
        item.items
          ? render(item.items)
          : createElement(
              "button",
              {
                key: item.id,
                onClick: item.onPress,
                disabled: item.disabled,
              },
              item.title,
            ),
      );
    return menus.map((menu) => render(menu.items));
  },
}));
vi.mock("../../components/ScreenHeaderButton", () => ({ ScreenHeaderButton: () => null }));
vi.mock("../../components/EmptyState", () => ({ EmptyState: () => null }));
vi.mock("../../components/LoadingScreen", () => ({ LoadingScreen: () => "Opening thread…" }));
vi.mock("./useThreadHeaderOptions", () => ({ useThreadHeaderOptions: () => ({}) }));
vi.mock("./HeadsUpInbox", () => ({ useHeadsUpInbox: () => ({ revision: 0 }) }));
vi.mock("./ThreadDetailScreen", () => ({ ThreadDetailScreen: () => "Chat" }));
vi.mock("./GitActionProgressOverlay", () => ({ GitActionProgressOverlay: () => null }));
vi.mock("./git/GitOverviewSheet", () => ({ GitOverviewSheet: () => null }));
vi.mock("../files/thread-file-navigator-pane", () => ({ ThreadFileNavigatorPane: () => null }));
vi.mock("./thread-inspector-content-stack", () => ({ ThreadInspectorContentStack: () => null }));
vi.mock("../layout/AdaptiveWorkspaceLayout", () => ({
  useAdaptiveWorkspaceLayout: () => ({
    fileInspector: { supported: false },
    layout: {},
    panes: {},
  }),
  useAdaptiveWorkspacePaneRole: () => {},
  useRegisterWorkspaceInspector: () => {},
}));
vi.mock("../settings/appearance/AppearancePreferencesProvider", () => ({
  useAppearancePreferences: () => ({ themeVariables: {} }),
}));
vi.mock("../../native/native-glass", () => ({ NATIVE_LIQUID_GLASS_SUPPORTED: false }));
vi.mock("../../state/workspace", () => ({ useConnectionsReady: () => true }));
vi.mock("../../state/shell", () => ({ useEnvironmentShellReadiness: () => ({ status: "live" }) }));
vi.mock("../../state/use-remote-environment-registry", () => ({
  useRemoteConnections: () => ({}),
  useRemoteConnectionStatus: () => ({ connectionState: "connected" }),
  useRemoteEnvironmentRuntime: () => ({ connectionState: "connected" }),
}));
vi.mock("../../state/use-thread-selection", () => ({
  useThreadSelection: () => ({
    selectedThread: { environmentId: ui.environmentId, id: ui.threadId, title: "Thread" },
    selectedThreadCreation: null,
    selectedThreadProject: null,
  }),
}));
vi.mock("../../state/use-thread-detail", async () => {
  const Option = await import("effect/Option");
  return {
    useSelectedThreadDetailState: () => ({
      data: Option.none(),
      error: Option.none(),
      status: "live",
      history: { hasMoreHistory: false, error: null },
    }),
  };
});
vi.mock("../../state/use-selected-thread-worktree", () => ({
  useSelectedThreadWorktree: () => ({ selectedThreadCwd: null }),
}));
vi.mock("../../state/use-selected-thread-git-state", () => ({
  useSelectedThreadGitState: () => ({}),
}));
vi.mock("../../state/use-selected-thread-git-actions", () => ({
  useSelectedThreadGitActions: () => ({}),
}));
vi.mock("../../state/use-selected-thread-requests", () => ({
  useSelectedThreadRequests: () => ({}),
}));
vi.mock("../../state/use-thread-composer-state", () => ({
  useThreadComposerState: () => ({
    selectedThreadFeed: [],
    activeWorkStartedAt: null,
    interruptibleRunId: null,
  }),
}));
vi.mock("../../state/use-terminal-session", () => ({
  useKnownTerminalSessions: () => ({ sessions: [] }),
}));
vi.mock("../../state/use-vcs-action-state", () => ({
  useGitActionProgress: () => null,
  dismissGitActionResult: () => {},
}));
vi.mock("./use-worktree-setup", () => ({
  useWorktreeSetup: () => ({ snapshot: null, visible: null }),
}));
vi.mock("../../state/session", () => ({
  useEnvironmentScope: () => ui.canOperate,
  readEnvironmentScope: () => ui.grantAtDispatch,
}));
vi.mock("../../state/threads", () => ({ threadEnvironment: { stopJob: "stop-job" } }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => ui.stop }));
vi.mock("../../state/vcs", () => ({ vcsEnvironment: {} }));
vi.mock("../../lib/commandMetadata", () => ({ makeTurnCommandMetadata: () => ({}) }));
vi.mock("../../state/recover-failed-thread-draft", () => ({ recoverFailedThreadDraft: () => {} }));
vi.mock("../../state/pending-thread-creation", () => ({
  clearPendingThreadCreationOutcome: () => {},
}));
vi.mock("../terminal/terminalLaunchContext", () => ({
  resolvePreferredThreadWorktreePath: () => null,
  stagePendingTerminalLaunch: () => {},
}));
vi.mock("../../lib/uuid", () => ({ uuidv4: () => "uuid" }));
vi.mock("./useThreadJobs", () => ({
  useThreadJobs: () => ({
    jobs: ui.jobs,
    isPending: false,
    error: null,
    hasMore: false,
  }),
}));
vi.mock("../../state/orchestration", () => ({
  orchestrationEnvironment: {
    turnItem: (target: DetailTarget) => {
      const key = JSON.stringify(target);
      let entry = ui.queries.get(key);
      if (!entry) {
        const response = Atom.make<DetailResult>(AsyncResult.initial(true));
        const query = Atom.make((get) => {
          ui.requests.push(target);
          return get(response);
        });
        entry = { query, response };
        ui.queries.set(key, entry);
      }
      return entry.query;
    },
  },
}));

import { ThreadRouteScreen } from "./ThreadRouteScreen";

type MenuItem = {
  id: string;
  title: string;
  onPress?: () => void;
  disabled?: boolean;
  items?: MenuItem[];
};
const environmentId = EnvironmentId.make("environment");
const threadId = ThreadId.make("thread");
function jobItem(running = false): Extract<OrchestrationV2TurnItem, { type: "system_notice" }> {
  const at = DateTime.makeUnsafe("2026-10-06T00:00:00.000Z");
  return {
    id: TurnItemId.make("old-job"),
    threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    type: "system_notice",
    message: "",
    title: "Old job",
    status: running ? "running" : "completed",
    startedAt: at,
    completedAt: running ? null : at,
    updatedAt: at,
    job: {
      version: 1,
      providerSessionId: ProviderSessionId.make("session"),
      scope: "live-runtime",
      id: "1",
      name: "Old job",
      command: "echo old",
      cwd: "/tmp",
      state: running ? "running" : "succeeded",
      startedAt: DateTime.toEpochMillis(at),
      endedAt: running ? null : DateTime.toEpochMillis(at) + 100,
      exitCode: running ? null : 0,
      signal: null,
      output: "Original output",
    },
  };
}
function summaries(item = jobItem()) {
  return threadJobs([{ ...item, job: { ...item.job!, output: "", outputOmitted: true } }]);
}
let root: Root;
let container: HTMLDivElement;
let registry: AtomRegistry.AtomRegistry;
const render = () =>
  act(async () =>
    root.render(
      <RegistryContext.Provider value={registry}>
        <ThreadRouteScreen
          route={{
            params: {
              environmentId: ui.environmentId,
              threadId: ui.threadId,
            },
          }}
        />
      </RegistryContext.Provider>,
    ),
  );
function button(label: string): HTMLButtonElement {
  const found = [...container.querySelectorAll("button")].find(
    (node) => node.textContent === label,
  );
  if (!found) throw new Error(`Button not found: ${label}`);
  return found;
}
const press = (label: string) => act(async () => button(label).click());
const respond = (result: DetailResult) =>
  act(async () => {
    const query = [...ui.queries.values()].at(-1)!;
    registry.set(query.response, result);
  });
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  ui.environmentId = environmentId;
  ui.threadId = threadId;
  ui.jobs = summaries();
  ui.queries.clear();
  ui.requests = [];
  ui.canOperate = true;
  ui.grantAtDispatch = true;
  ui.stop.mockClear();
  ui.alert.mockClear();
  registry = AtomRegistry.make();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  registry.dispose();
  container.remove();
  vi.unstubAllGlobals();
});

it("keeps a selected older job open when page one resets, and does not reopen it after Back", async () => {
  await render();
  expect(ui.requests).toEqual([]); // Summaries never fetch output before selection.
  await press("Old job");
  await respond(AsyncResult.success({ item: jobItem() }));
  expect(container.textContent).toContain("Original output");

  ui.jobs = []; // Fresh page one no longer contains the finished page-two row.
  await render();
  expect(container.textContent).not.toContain("Chat");
  expect(container.textContent).toContain("Loading job…");
  await respond(AsyncResult.success({ item: jobItem() }));
  expect(container.textContent).toContain("Original output");
  await press("← Back to thread");
  expect(container.textContent).toContain("Chat");
  ui.jobs = summaries();
  await render();
  expect(container.textContent).toContain("Chat");
  expect(container.textContent).not.toContain("Original output");
});

it.each(["environment", "thread"] as const)(
  "does not carry or resurrect selection across %s changes",
  async (scope) => {
    await render();
    await press("Old job");
    await respond(AsyncResult.success({ item: jobItem() }));
    if (scope === "environment") ui.environmentId = "other-environment";
    else ui.threadId = "other-thread";
    await render();
    expect(container.textContent).toContain("Chat");
    expect(container.textContent).not.toContain("Original output");
    const otherScope = { environmentId: ui.environmentId, threadId: ui.threadId };
    ui.environmentId = environmentId;
    ui.threadId = threadId;
    await render();
    expect(container.textContent).toContain("Chat");
    expect(container.textContent).not.toContain("Original output");
    Object.assign(ui, otherScope);
    await render();
    await press("Old job");
    const target = ui.requests.at(-1)!;
    expect(target.environmentId).toBe(ui.environmentId);
    expect(target.input.threadId).toBe(ui.threadId);
    await respond(AsyncResult.success({ item: null }));
    expect(container.textContent).not.toContain("Original output");
    ui.environmentId = environmentId;
    ui.threadId = threadId;
    await render();
    expect(container.textContent).toContain("Chat");
    expect(container.textContent).not.toContain("Original output");
  },
);

it("shows loading, retryable errors and missing jobs without returning to chat", async () => {
  await render();
  await press("Old job");
  ui.jobs = [];
  await render();
  expect(container.textContent).toContain("Loading job…");
  await respond(AsyncResult.failure(Cause.fail(new Error("Disconnected"))));
  expect(container.textContent).toContain("Could not load job.");
  const requests = ui.requests.length;
  await press("Retry loading job");
  expect(ui.requests.length).toBeGreaterThan(requests);
  await respond(AsyncResult.success({ item: null }));
  expect(container.textContent).toContain("This job is no longer available.");
  expect(container.textContent).not.toContain("Chat");
  await press("← Back to thread");
  expect(container.textContent).toContain("Chat");
});

it("refreshes selected output on a live revision and respects Stop grants, including revocation during confirmation", async () => {
  ui.jobs = summaries(jobItem(true));
  ui.canOperate = false;
  await render();
  await press("Old job");
  await respond(AsyncResult.success({ item: jobItem(true) }));
  expect(button("Stop job").disabled).toBe(true);
  ui.canOperate = true;
  await render();
  await press("Stop job");
  const confirm = ui.alert.mock.calls.at(-1)![2]!.find((action) => action.text === "Stop job")!;
  ui.grantAtDispatch = false;
  await act(async () => confirm.onPress!());
  expect(ui.stop).not.toHaveBeenCalled();
  ui.grantAtDispatch = true;
  await act(async () => confirm.onPress!());
  expect(ui.stop).toHaveBeenCalledWith({
    environmentId,
    input: { threadId, turnItemId: TurnItemId.make("old-job") },
  });

  const completed = { ...jobItem(), updatedAt: DateTime.makeUnsafe("2026-10-06T00:01:00.000Z") };
  ui.jobs = summaries(completed);
  await render();
  expect(container.textContent).toContain("Succeeded");
  expect(container.textContent).toContain("Loading output…");
  expect(container.textContent).not.toContain("Stop job");
  await respond(
    AsyncResult.success({
      item: { ...completed, job: { ...completed.job!, output: "Final output" } },
    }),
  );
  expect(container.textContent).toContain("Final output");
});
