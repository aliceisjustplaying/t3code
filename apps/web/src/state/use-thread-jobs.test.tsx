// @vitest-environment jsdom
import { RegistryContext } from "@effect/atom-react";
import { it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderSessionId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2GetJobsPageInput,
  type OrchestrationV2GetJobsPageResult,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import {
  EnvironmentRegistry,
  EnvironmentSupervisor,
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type ConnectionCatalogEntry,
  type NetworkStatus,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "@t3tools/client-runtime/connection";
import type { RpcSession } from "@t3tools/client-runtime/rpc";
import { createEnvironmentQueryAtomFamily } from "@t3tools/client-runtime/state/runtime";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { expect, vi } from "vite-plus/test";

// Keep React's async act scope open while the test fiber runs the effect. The
// release flushes React updates on success, failure or interruption.
const actEffect = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      let finish!: () => void;
      const pending = new Promise<void>((resolve) => {
        finish = resolve;
      });
      const flushed = Promise.resolve(act(() => pending));
      return { finish, flushed };
    }),
    () => effect,
    ({ finish, flushed }) =>
      Effect.promise(() => {
        finish();
        return flushed;
      }),
  );

type Target = { environmentId: EnvironmentId; input: OrchestrationV2GetJobsPageInput };
const queries = vi.hoisted(() => ({
  jobsPage: (
    _target: Target,
  ): Atom.Atom<AsyncResult.AsyncResult<OrchestrationV2GetJobsPageResult, Error>> => {
    throw new Error("Test transport is not initialized");
  },
}));
vi.mock("./orchestration", () => ({ orchestrationEnvironment: queries }));
import { useThreadJobs } from "./use-thread-jobs";

const environmentId = EnvironmentId.make("environment");
const threadId = ThreadId.make("thread");
const connectionState: SupervisorConnectionState = {
  ...AVAILABLE_CONNECTION_STATE,
  desired: true,
  network: "online",
  phase: "connected",
  attempt: 1,
  generation: 1,
};

function item(ordinal: number, running = false): OrchestrationV2TurnItem {
  const at = DateTime.makeUnsafe(ordinal * 1000);
  return {
    id: TurnItemId.make(`job-${ordinal}`),
    threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    type: "system_notice",
    message: "",
    status: running ? "running" : "completed",
    title: `Job ${ordinal}`,
    startedAt: at,
    completedAt: running ? null : at,
    updatedAt: at,
    job: {
      version: 1,
      providerSessionId: ProviderSessionId.make("session"),
      scope: "live-runtime",
      id: `${ordinal}`,
      name: `Job ${ordinal}`,
      command: "echo test",
      cwd: "/tmp",
      state: running ? "running" : "succeeded",
      startedAt: ordinal * 1000,
      endedAt: running ? null : ordinal * 1000 + 100,
      exitCode: running ? null : 0,
      signal: null,
      output: "",
      outputOmitted: true,
    },
  };
}
const page = (
  items: OrchestrationV2TurnItem[],
  hasMore = true,
): OrchestrationV2GetJobsPageResult => {
  const last = items.at(-1)!;
  return { items, nextCursor: hasMore ? { ordinal: last.ordinal, itemId: last.id } : null };
};

it.effect(
  "replaces loaded older pages on refresh, snapshot replacement and reconnect without crossing scopes",
  () =>
    Effect.gen(function* () {
      vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
      const registry = AtomRegistry.make();
      const requests = yield* Queue.unbounded<{
        target: Target;
        reply: Deferred.Deferred<OrchestrationV2GetJobsPageResult>;
      }>();
      const state = yield* SubscriptionRef.make(connectionState);
      const session = yield* SubscriptionRef.make<Option.Option<RpcSession>>(
        Option.some({} as RpcSession),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: new PrimaryConnectionTarget({
          environmentId,
          label: "Test",
          httpBaseUrl: "https://test.invalid",
          wsBaseUrl: "wss://test.invalid",
        }),
        state,
        session,
        prepared: yield* SubscriptionRef.make<Option.Option<PreparedConnection>>(Option.none()),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      });
      const unsupported = () => Effect.die(new Error("Unexpected registry operation"));
      const environmentRegistry = EnvironmentRegistry.EnvironmentRegistry.of({
        entries: yield* SubscriptionRef.make<ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>>(
          new Map(),
        ),
        networkStatus: yield* SubscriptionRef.make<NetworkStatus>("online"),
        start: unsupported(),
        register: unsupported,
        registerPlatform: unsupported,
        reconcilePlatform: unsupported,
        remove: unsupported,
        removeRoute: unsupported,
        reorderRoutes: unsupported,
        removeRelayEnvironments: unsupported,
        retryNow: unsupported,
        setEnabled: unsupported,
        setCompatibility: unsupported,
        state: () => SubscriptionRef.get(state),
        runStream: (_id, stream) =>
          Stream.provideService(stream, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        run: (_id, effect) =>
          Effect.provideService(effect, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        followStream: (_id, stream) =>
          Stream.provideService(stream, EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        stateChanges: () => SubscriptionRef.changes(state),
      });
      const family = createEnvironmentQueryAtomFamily(
        Atom.runtime(Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environmentRegistry)),
        {
          label: "test.jobs-page",
          staleTimeMs: 0,
          idleTtlMs: 0,
          execute: (input: Target) =>
            Effect.gen(function* () {
              const reply = yield* Deferred.make<OrchestrationV2GetJobsPageResult>();
              yield* Queue.offer(requests, { target: input, reply });
              return yield* Deferred.await(reply);
            }),
        },
      );
      queries.jobsPage = (target) => family({ environmentId: target.environmentId, input: target });
      const container = document.createElement("div");
      document.body.append(container);
      const root = createRoot(container);
      let view!: ReturnType<typeof useThreadJobs>;
      let target = { environmentId, threadId };
      let live: OrchestrationV2TurnItem[] = [item(20, true)];
      function Probe() {
        view = useThreadJobs(target, live);
        return <>{view.jobs.map((job) => `${job.name}:${job.state}`).join(",")}</>;
      }
      const render = () =>
        Effect.promise(async () => {
          await act(async () =>
            root.render(
              <RegistryContext.Provider value={registry}>
                <Probe />
              </RegistryContext.Provider>,
            ),
          );
        });
      const next = () => Queue.take(requests);
      const respond = (
        request: { target: Target; reply: Deferred.Deferred<OrchestrationV2GetJobsPageResult> },
        data: OrchestrationV2GetJobsPageResult,
      ) =>
        actEffect(
          Effect.gen(function* () {
            yield* Deferred.succeed(request.reply, data);
            yield* AtomRegistry.getResult(registry, queries.jobsPage(request.target), {
              suspendOnWaiting: true,
            });
          }),
        );
      const disconnect = () =>
        actEffect(
          SubscriptionRef.set(state, {
            ...connectionState,
            phase: "backoff",
          } satisfies SupervisorConnectionState),
        );
      let generation = 1;
      const reconnect = () =>
        actEffect(SubscriptionRef.set(state, { ...connectionState, generation: ++generation }));
      try {
        yield* render();
        const initial = yield* next();
        expect(initial.target.input.cursor).toBeNull();
        yield* respond(initial, page([item(30), item(20, true)]));
        yield* Effect.promise(async () => {
          await act(async () => view.loadOlder());
        });
        const older = yield* next();
        expect(older.target.input.cursor?.ordinal).toBe(20);
        yield* respond(older, page([item(10)], false));
        expect(container.textContent).toBe("Job 30:succeeded,Job 20:running,Job 10:succeeded");

        // J finishes offline. The authoritative bounded snapshot omits J completely.
        yield* disconnect();
        live = [];
        yield* render();
        yield* reconnect();
        const replacement = yield* next();
        expect(replacement.target.input.cursor).toBeNull();
        yield* respond(replacement, page([item(40), item(30)]));
        expect(container.textContent).toBe("Job 40:succeeded,Job 30:succeeded");
        yield* Effect.promise(async () => {
          await act(async () => view.loadOlder());
        });
        yield* respond(yield* next(), page([item(20), item(10)], false));
        expect(container.textContent).toBe(
          "Job 40:succeeded,Job 30:succeeded,Job 20:succeeded,Job 10:succeeded",
        );

        // No live job revision changes this time: the real query runtime still
        // refreshes page one on reconnect. An older response cannot retain stale rows.
        yield* disconnect();
        yield* reconnect();
        const a = yield* next();
        const b = yield* next();
        const freshFirst = a.target.input.cursor == null ? a : b;
        const staleOlder = a.target.input.cursor == null ? b : a;
        yield* respond(staleOlder, page([item(20), item(10)], false));
        yield* respond(freshFirst, page([item(50), item(40)]));
        expect(container.textContent).toBe("Job 50:succeeded,Job 40:succeeded");

        yield* Effect.promise(async () => {
          await act(async () => view.loadOlder());
        });
        yield* respond(yield* next(), page([item(30)], false));
        yield* Effect.promise(async () => {
          await act(async () => view.refresh());
        });
        const refreshed = yield* next();
        expect(refreshed.target.input.cursor).toBeNull();
        yield* respond(refreshed, page([item(60), item(50)]));
        expect(container.textContent).toBe("Job 60:succeeded,Job 50:succeeded");

        target = { environmentId: EnvironmentId.make("other-environment"), threadId };
        yield* render();
        const otherEnvironment = yield* next();
        expect(otherEnvironment.target.environmentId).toBe(target.environmentId);
        expect(otherEnvironment.target.input.cursor).toBeNull();
        yield* respond(otherEnvironment, page([item(5)], false));
        expect(container.textContent).toBe("Job 5:succeeded");
        target = { ...target, threadId: ThreadId.make("other-thread") };
        yield* render();
        const otherThread = yield* next();
        expect(otherThread.target.input.threadId).toBe(target.threadId);
        expect(otherThread.target.input.cursor).toBeNull();
        yield* respond(otherThread, page([{ ...item(1), threadId: target.threadId }], false));
        expect(container.textContent).toBe("Job 1:succeeded");
      } finally {
        yield* Effect.promise(async () => {
          await act(async () => root.unmount());
        });
        registry.dispose();
        container.remove();
        yield* Queue.shutdown(requests);
        vi.unstubAllGlobals();
      }
    }),
);
