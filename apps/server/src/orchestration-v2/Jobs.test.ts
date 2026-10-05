import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  TurnItemId,
  ProviderInstanceId,
  ProviderDriverKind,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Recovery from "./ProviderRuntimeRecoveryService.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("pi");
const database = SqlitePersistenceMemory;
const stores = Layer.mergeAll(ProjectionStore.layer, EventStore.layer, EffectOutbox.layer).pipe(
  Layer.provide(database),
);
const recoveryLayer = Recovery.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      stores,
      IdAllocator.layer,
      ServerSettings.layerTest(),
      EventSink.layer.pipe(Layer.provide(Layer.merge(database, stores))),
    ),
  ),
);
const layer = Layer.mergeAll(
  recoveryLayer,
  database,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  EffectOutbox.layer.pipe(Layer.provide(database)),
  makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "jobs" },
    ProviderAdapterRegistry.makeLayer([
      {
        instanceId,
        driver: ProviderDriverKind.make("pi"),
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
        openSession: () => Effect.die("Stop must not create a provider process"),
      } satisfies ProviderAdapterV2Shape,
    ]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);

it.effect(
  "jobs survive transcript paging, scope stop to the source and retire stop after process loss",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("job-owner");
      const sourceThreadId = ThreadId.make("job-child");
      const providerThreadId = ProviderThreadId.make("job-provider-thread");
      const providerSessionId = ProviderSessionId.make("job-session");
      const now = yield* DateTime.now;
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create-jobs"),
        threadId,
        projectId: ProjectId.make("jobs-project"),
        title: "Jobs",
        modelSelection: { instanceId, model: "default" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const item: OrchestrationV2TurnItem = {
        id: TurnItemId.make("job-item"),
        threadId,
        runId: null,
        nodeId: null,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 0,
        status: "running",
        title: "Build",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "system_notice",
        message: "",
        job: {
          version: 1,
          scope: "live-runtime",
          id: "1",
          name: "Build",
          command: "make",
          cwd: "/tmp",
          state: "running",
          startedAt: DateTime.toEpochMillis(now),
          endedAt: null,
          exitCode: null,
          signal: null,
          output: "build output",
          providerSessionId,
          sourceThreadId,
        },
      };
      yield* projections.apply({
        id: EventId.make("job-started"),
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: item,
      });
      yield* projections.apply({
        id: EventId.make("newer-item"),
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: {
          ...item,
          id: TurnItemId.make("plain-notice"),
          ordinal: 10,
          job: undefined,
          status: "completed",
          message: "Newer",
        },
      });
      const snapshot = yield* projections.getThreadSnapshotWindow(threadId, { rowLimit: 1 });
      assert.isTrue(snapshot.projection.turnItems.some((candidate) => candidate.id === item.id));
      const commandId = CommandId.make("stop-job");
      yield* orchestrator.dispatch({
        type: "thread.job.stop",
        commandId,
        threadId,
        turnItemId: item.id,
      });
      const effects = yield* outbox.listByCommandId(commandId);
      assert.equal(effects[0]?.threadId, sourceThreadId);
      assert.deepEqual(effects[0]?.request, {
        type: "provider-job.stop",
        providerThreadId,
        providerSessionId,
        scope: "live-runtime",
        jobId: "1",
      });
      yield* outbox.reconcileAfterProcessLoss;
      assert.equal((yield* outbox.listByCommandId(commandId))[0]?.status, "cancelled");
      yield* projections.apply({
        id: EventId.make("job-completed"),
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: {
          ...item,
          status: "completed",
          completedAt: now,
          job: {
            ...item.job!,
            state: "succeeded",
            endedAt: DateTime.toEpochMillis(now),
            exitCode: 0,
          },
        },
      });
      const endedStop = yield* Effect.exit(
        orchestrator.dispatch({
          type: "thread.job.stop",
          commandId: CommandId.make("stop-ended"),
          threadId,
          turnItemId: item.id,
        }),
      );
      assert.equal(endedStop._tag, "Failure");
      const lostId = TurnItemId.make("job-lost");
      yield* projections.apply({
        id: EventId.make("orphan-job"),
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: { ...item, id: lostId, ordinal: 20 },
      });
      assert.include(yield* projections.getRecoveryThreadIds("runtime"), threadId);
      const recovery = yield* Recovery.ProviderRuntimeRecoveryService;
      yield* recovery.recover;
      const restored = yield* projections.getThreadSnapshotWindow(threadId, { rowLimit: 1 });
      assert.equal(
        restored.projection.turnItems.find((candidate) => candidate.id === lostId)?.status,
        "cancelled",
      );
      assert.equal(
        restored.projection.turnItems.find((candidate) => candidate.id === item.id)?.status,
        "completed",
      );
    }).pipe(Effect.provide(layer)),
);
