// @vitest-environment jsdom
import { RegistryContext } from "@effect/atom-react";
import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { vi } from "vite-plus/test";
import type {
  EnvironmentId,
  OrchestrationV2GetJobsPageInput,
  OrchestrationV2GetJobsPageResult,
} from "@t3tools/contracts";
import type { AsyncResult, Atom } from "effect/reactivity";
import { testThreadJobsPagination } from "./thread-jobs-test";

const queries = vi.hoisted(() => ({
  jobsPage: (_target: {
    environmentId: EnvironmentId;
    input: OrchestrationV2GetJobsPageInput;
  }): Atom.Atom<AsyncResult.AsyncResult<OrchestrationV2GetJobsPageResult, Error>> => {
    throw new Error("Test transport is not initialized");
  },
}));
vi.mock("./orchestration", () => ({ orchestrationEnvironment: queries }));
import { useThreadJobs } from "./use-thread-jobs";

testThreadJobsPagination({
  useThreadJobs,
  queries,
  act,
  createElement,
  createRoot,
  RegistryContext,
});
