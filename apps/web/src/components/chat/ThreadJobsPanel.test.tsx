// @vitest-environment jsdom
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId, type OrchestrationV2TurnItem } from "@t3tools/contracts";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";

const state = vi.hoisted(() => ({ items: [] as OrchestrationV2TurnItem[] }));
vi.mock("../../state/entities", () => ({
  useThreadProjection: () => ({ projection: { turnItems: state.items } }),
}));
vi.mock("../../state/threads", () => ({ threadEnvironment: { stopJob: {} } }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));

import { ThreadJobsPanel } from "./ThreadJobsPanel";

let root: Root;
let container: HTMLElement;
const onSelect = vi.fn();
const render = async () => {
  await act(async () =>
    root.render(
      <ThreadJobsPanel
        environmentId={EnvironmentId.make("environment")}
        threadId={ThreadId.make("thread")}
        onSelect={onSelect}
      />,
    ),
  );
};
const button = (text: string) => {
  const found = [...container.querySelectorAll("button")].find((candidate) =>
    candidate.textContent?.startsWith(text),
  );
  if (!found) throw new Error(`Missing button: ${text}`);
  return found;
};
const jobNames = () =>
  [...container.querySelectorAll("button[title]")].map((candidate) =>
    candidate.getAttribute("title"),
  );
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  onSelect.mockClear();
  state.items = [
    { id: "running", state: "running", startedAt: 10 },
    { id: "failed", state: "failed", startedAt: 20 },
    { id: "finished", state: "succeeded", startedAt: 40 },
    { id: "timeout", state: "timed_out", startedAt: 30 },
  ].map(({ id, ...job }) => ({
    id,
    type: "system_notice",
    status: "completed",
    job: { ...job, name: id, endedAt: job.state === "running" ? null : 50 },
  })) as OrchestrationV2TurnItem[];
  container = document.createElement("div");
  document.body.appendChild(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

it("collapses all job controls and restores the finished filter, order and selection on reopening", async () => {
  await render();
  expect(jobNames()).toEqual(["timeout", "failed", "running"]);
  await act(async () => button("Show finished").click());
  expect(jobNames()).toEqual(["finished", "timeout", "failed", "running"]);

  await act(async () => button("Jobs").click());
  expect(button("Jobs").getAttribute("aria-expanded")).toBe("false");
  expect(container.textContent).toContain("1 running");
  expect(container.querySelectorAll("button")).toHaveLength(1);
  expect(onSelect).not.toHaveBeenCalled();

  await act(async () => button("Jobs").click());
  expect(button("Jobs").getAttribute("aria-expanded")).toBe("true");
  expect(jobNames()).toEqual(["finished", "timeout", "failed", "running"]);
  await act(async () => button("finished").click());
  expect(onSelect).toHaveBeenCalledWith("finished");
  await act(async () => button("Hide finished").click());
  expect(jobNames()).toEqual(["timeout", "failed", "running"]);
});
