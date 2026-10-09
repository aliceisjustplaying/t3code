// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vite-plus/test";
import {
  EnvironmentId,
  ProviderSessionId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2TurnItem,
  type OrchestrationV2Job,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";

const state = vi.hoisted(() => ({
  items: [] as OrchestrationV2TurnItem[],
  detail: null as OrchestrationV2TurnItem | null,
  deniedEnvironment: null as EnvironmentId | null,
  stop: vi.fn(async (_input: unknown) => ({ _tag: "Success" })),
  first: {
    items: [] as OrchestrationV2TurnItem[],
    nextCursor: null as { ordinal: number; itemId: TurnItemId } | null,
  },
  older: { items: [] as OrchestrationV2TurnItem[], nextCursor: null },
  calls: [] as { environmentId: EnvironmentId; input: { threadId: ThreadId; cursor?: unknown } }[],
}));
vi.mock("../../state/orchestration", () => ({
  orchestrationEnvironment: {
    jobsPage: (target: {
      environmentId: EnvironmentId;
      input: { threadId: ThreadId; cursor?: unknown };
    }) => {
      state.calls.push(target);
      return target.input.cursor == null ? state.first : state.older;
    },
  },
}));
vi.mock("../../state/query", () => ({
  useEnvironmentQuery: (data: unknown) => ({
    data,
    error: null,
    isPending: false,
    refresh: () => {},
  }),
}));
vi.mock("../../state/queries", () => ({
  useTurnItemDetail: () => {
    if (!state.detail) throw new Error("Job output must not load while only the list is open");
    return { data: { item: state.detail }, isPending: false, error: null, refresh: () => {} };
  },
}));
vi.mock("../../state/entities", () => ({
  useThreadProjection: () => ({ projection: { turnItems: state.items } }),
}));
vi.mock("../../state/session", () => ({
  useEnvironmentScope: (environment: EnvironmentId) => environment !== state.deniedEnvironment,
  readEnvironmentScope: (environment: EnvironmentId) => environment !== state.deniedEnvironment,
}));
vi.mock("../../state/threads", () => ({ threadEnvironment: { stopJob: null } }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => state.stop }));
import { ThreadJobDetails, ThreadJobsPanel } from "./ThreadJobsPanel";
const threadId = ThreadId.make("thread");
function item(index: number, status: OrchestrationV2Job["state"]): OrchestrationV2TurnItem {
  const at = DateTime.makeUnsafe(index * 1000);
  return {
    id: TurnItemId.make(`job-${index}`),
    threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: index,
    type: "system_notice",
    message: "",
    status: "completed",
    title: `Job ${index}`,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
    job: {
      version: 1,
      providerSessionId: ProviderSessionId.make("session"),
      scope: "live-runtime",
      id: `${index}`,
      name: `Job ${index}`,
      command: "echo test",
      cwd: "/tmp",
      state: status,
      startedAt: index * 1000,
      endedAt: index * 1000 + 100,
      exitCode: status === "failed" ? 1 : 0,
      signal: null,
      output: "",
    },
  };
}
let root: Root | undefined;
let container: HTMLElement | undefined;
afterEach(async () => {
  if (root) await act(async () => root!.unmount());
  container?.remove();
  vi.unstubAllGlobals();
  state.detail = null;
  state.deniedEnvironment = null;
  state.stop.mockClear();
});
it("starts collapsed, toggles jobs and loads older summaries in their destination environment", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  state.items = [];
  state.calls = [];
  state.first = {
    items: [
      item(5, "succeeded"),
      item(3, "timed_out"),
      item(6, "succeeded"),
      item(2, "stopped"),
      item(4, "succeeded"),
    ],
    nextCursor: { ordinal: 2, itemId: TurnItemId.make("job-2") },
  };
  state.older = { items: [item(1, "failed")], nextCursor: null };
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  await act(async () =>
    root!.render(
      <ThreadJobsPanel
        environmentId={EnvironmentId.make("environment")}
        threadId={threadId}
        onSelect={() => {}}
      />,
    ),
  );
  const names = () =>
    [...container!.querySelectorAll<HTMLButtonElement>("button[title]")].map(
      (button) => button.title,
    );
  const sectionToggle = () =>
    [...container!.querySelectorAll<HTMLButtonElement>("button")].find((button) =>
      button.textContent?.startsWith("Jobs"),
    )!;
  expect(names()).toEqual([]);
  expect(sectionToggle().textContent).toContain("Jobs (5+)");
  await act(async () => sectionToggle().click());
  expect(names()).toEqual(["Job 6", "Job 5", "Job 4", "Job 3", "Job 2"]);
  const toggle = () =>
    [...container!.querySelectorAll<HTMLButtonElement>("button")].find((button) =>
      button.textContent?.includes("older jobs ("),
    )!;
  const load = [...container!.querySelectorAll<HTMLButtonElement>("button")].find(
    (button) => button.textContent === "Load older jobs",
  )!;
  await act(async () => load.click());
  expect(names()).toEqual(["Job 6", "Job 5", "Job 4", "Job 3", "Job 2", "Job 1"]);
  expect(state.calls.some((call) => call.input.cursor === state.first.nextCursor)).toBe(true);
  expect(
    state.calls.every(
      (call) =>
        call.environmentId === EnvironmentId.make("environment") &&
        call.input.threadId === threadId,
    ),
  ).toBe(true);
  await act(async () => toggle().click());
  expect(names()).toEqual(["Job 6", "Job 5", "Job 4", "Job 3", "Job 2"]);
  await act(async () => sectionToggle().click());
  expect(names()).toEqual([]);
  expect(sectionToggle().textContent).toContain("Jobs (6)");
  await act(async () => sectionToggle().click());
  expect(names()).toEqual(["Job 6", "Job 5", "Job 4", "Job 3", "Job 2"]);
});

it("job stop uses the destination grant and rechecks it when confirming", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const environmentId = EnvironmentId.make("destination");
  const job = item(1, "running");
  state.detail = job;
  state.items = [job];
  state.deniedEnvironment = environmentId;
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  const render = () =>
    act(async () =>
      root!.render(
        <ThreadJobDetails
          environmentId={environmentId}
          threadId={threadId}
          itemId={job.id}
          onClose={() => {}}
        />,
      ),
    );
  await render();
  const stopButton = () =>
    [...container!.querySelectorAll<HTMLButtonElement>("button")].find(
      (button) => button.textContent === "Stop job",
    )!;
  expect(stopButton().disabled).toBe(true);
  state.deniedEnvironment = null;
  await render();
  await act(async () => stopButton().click());
  const confirm = [
    ...document.querySelectorAll<HTMLButtonElement>('[role="alertdialog"] button'),
  ].find((button) => button.textContent === "Stop job")!;
  // Permission can change after the dialog opens, before React receives an update.
  state.deniedEnvironment = environmentId;
  await act(async () => confirm.click());
  expect(state.stop).not.toHaveBeenCalled();
  state.deniedEnvironment = null;
  await act(async () => confirm.click());
  expect(state.stop).toHaveBeenCalledWith({
    environmentId,
    input: { threadId, turnItemId: job.id },
  });
});
