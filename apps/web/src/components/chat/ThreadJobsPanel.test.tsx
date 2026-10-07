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

const state = vi.hoisted(() => ({ items: [] as OrchestrationV2TurnItem[] }));
vi.mock("../../state/entities", () => ({
  useThreadProjection: () => ({ projection: { turnItems: state.items } }),
}));
vi.mock("../../state/threads", () => ({ threadEnvironment: { stopJob: null } }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
import { ThreadJobsPanel } from "./ThreadJobsPanel";
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
});
it("shows newest jobs regardless of outcome, with older jobs expandable in the same order", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  state.items = [
    item(1, "failed"),
    item(5, "succeeded"),
    item(3, "timed_out"),
    item(6, "succeeded"),
    item(2, "stopped"),
    item(4, "succeeded"),
  ];
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
  expect(names()).toEqual(["Job 6", "Job 5", "Job 4", "Job 3", "Job 2"]);
  const toggle = () =>
    [...container!.querySelectorAll<HTMLButtonElement>("button")].find((button) =>
      button.hasAttribute("aria-expanded"),
    )!;
  await act(async () => toggle().click());
  expect(names()).toEqual(["Job 6", "Job 5", "Job 4", "Job 3", "Job 2", "Job 1"]);
  await act(async () => toggle().click());
  expect(names()).toEqual(["Job 6", "Job 5", "Job 4", "Job 3", "Job 2"]);
});
