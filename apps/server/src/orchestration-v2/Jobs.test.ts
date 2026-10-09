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
  type OrchestrationV2GetJobsPageResult,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Statement from "effect/sql/Statement";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Recovery from "./ProviderRuntimeRecoveryService.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ServerSettings from "../serverSettings.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import { buildBoundedThreadStreamSnapshot } from "./ThreadStream.ts";
import { THREAD_HISTORY_PAGE_POLICY } from "./threadHistoryPaging.ts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("pi");
const database = SqlitePersistence.layerMemory;
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
  ProviderReplayHarness.layerWithRegistry(
    { name: "jobs" },
    ProviderAdapterRegistry.layerFromAdapters([
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
      assert.isFalse(restored.projection.turnItems.some((candidate) => candidate.id === lostId));
      assert.equal(
        (yield* projections.getTurnItem({ threadId, itemId: lostId }))?.status,
        "cancelled",
      );
      assert.equal(
        (yield* projections.getTurnItem({ threadId, itemId: item.id }))?.status,
        "completed",
      );
    }).pipe(Effect.provide(layer)),
);

const jobFixture = (
  threadId: ThreadId,
  ordinal: number,
  now: DateTime.Utc,
): Extract<OrchestrationV2TurnItem, { type: "system_notice" }> => ({
  id: TurnItemId.make("job-" + ordinal),
  threadId,
  ordinal,
  runId: null,
  nodeId: null,
  providerThreadId: ProviderThreadId.make("job-provider"),
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  status: ordinal < 2 ? "running" : "completed",
  title: "Build " + ordinal,
  startedAt: now,
  completedAt: ordinal < 2 ? null : now,
  updatedAt: now,
  type: "system_notice",
  message: "",
  job: {
    version: 1,
    scope: "live-runtime",
    id: String(ordinal + 1),
    name: "Build " + ordinal,
    command: "make",
    cwd: "/tmp",
    state: ordinal < 2 ? "running" : "succeeded",
    startedAt: DateTime.toEpochMillis(now),
    endedAt: ordinal < 2 ? null : DateTime.toEpochMillis(now),
    exitCode: ordinal < 2 ? null : 0,
    signal: null,
    output: "界".repeat(16_000),
    providerSessionId: ProviderSessionId.make("job-session"),
    ...(ordinal % 2 ? { sourceThreadId: ThreadId.make("child"), sourceTitle: "Child" } : {}),
  },
});
const createJobsThread = (
  orchestrator: Orchestrator.OrchestratorV2["Service"],
  threadId: ThreadId,
) =>
  orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create-" + threadId),
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

it.effect(
  "pages every persisted job including old forwarded summaries without transferring output until detail selection",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const service = yield* ThreadManagementService.ThreadManagementService;
      const threadId = ThreadId.make("many-jobs");
      const now = yield* DateTime.now;
      yield* createJobsThread(orchestrator, threadId);
      const memory = yield* ProjectionStore.ProjectionStoreV2.pipe(
        Effect.provide(ProjectionStore.layerMemory),
      );
      yield* memory.apply({
        id: EventId.make("memory-thread"),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: (yield* projections.getThreadProjection(threadId)).thread,
      });
      for (let ordinal = 0; ordinal < 220; ordinal++) {
        const event = {
          id: EventId.make("job-" + ordinal),
          type: "turn-item.updated" as const,
          threadId,
          occurredAt: now,
          // Equal ordinals straddle the first page: the ID tie-breaker must not skip either job.
          payload: {
            ...jobFixture(threadId, ordinal, now),
            ordinal: ordinal === 194 ? 195 : ordinal,
            // SQLite orders these case-sensitive IDs differently from localeCompare.
            id: TurnItemId.make(
              ordinal === 194 ? "job-Z" : ordinal === 195 ? "job-a" : "job-" + ordinal,
            ),
          },
        };
        yield* projections.apply(event);
        yield* memory.apply(event);
      }
      // Unrelated newer activity pushes all jobs outside the recent transcript.
      for (let ordinal = 220; ordinal < 320; ordinal++) {
        const { job: _job, ...notice } = jobFixture(threadId, ordinal, now);
        yield* projections.apply({
          id: EventId.make("notice-" + ordinal),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: { ...notice, message: "Newer activity" },
        });
      }
      const window = yield* projections.getThreadSnapshotWindow(threadId, { rowLimit: 77 });
      const snapshot = buildBoundedThreadStreamSnapshot(window);
      const retained = snapshot.projection.turnItems.filter(
        (item) => item.type === "system_notice" && item.job !== undefined,
      );
      assert.deepEqual(
        retained.map((item) => item.id),
        [TurnItemId.make("job-0"), TurnItemId.make("job-1")],
      );
      assert.isBelow(
        Buffer.byteLength(JSON.stringify(snapshot), "utf8"),
        THREAD_HISTORY_PAGE_POLICY.maxEncodedBytes,
      );
      assert.isFalse(snapshot.payloadBudgetExceeded);
      assert.isTrue(snapshot.hasMoreHistory);
      assert.notInclude(JSON.stringify(snapshot), "界");

      const queries: Array<readonly [string, ReadonlyArray<unknown>]> = [];
      const record: Statement.Transformer = (statement) =>
        Effect.sync(() => {
          queries.push(statement.compile());
          return statement;
        });
      const all: OrchestrationV2TurnItem[] = [];
      let cursor: import("@t3tools/contracts").OrchestrationV2GetJobsPageInput["cursor"] = null;
      for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
        const page: OrchestrationV2GetJobsPageResult = yield* service
          .getJobsPage({ threadId, cursor })
          .pipe(Effect.provideService(Statement.CurrentTransformer, record));
        const memoryPage = yield* memory.getJobsPage(threadId, {
          beforeJob: cursor ?? undefined,
          limit: 25,
        });
        assert.deepEqual(page.items, memoryPage.items);
        assert.equal(page.nextCursor !== null, memoryPage.hasMore);
        assert.isAtMost(page.items.length, 25);
        assert.notInclude(JSON.stringify(page), "界");
        all.push(...page.items);
        cursor = page.nextCursor;
        if (cursor === null) break;
      }
      assert.isNull(cursor);
      // Every actual persisted jobs read is bounded and avoids a whole-history count.
      const jobsQueries = queries.filter(([query]) => query.includes("$.job.id"));
      assert.isNotEmpty(jobsQueries);
      for (const [query, params] of jobsQueries) {
        assert.notInclude(query.toUpperCase(), "COUNT(");
        assert.include(query.toUpperCase(), "LIMIT");
        assert.equal(params.at(-1), 26);
      }
      assert.deepEqual(
        all.map((item) => item.id),
        Array.from({ length: 220 }, (_, index) => {
          const ordinal = 219 - index;
          return TurnItemId.make(
            ordinal === 194 ? "job-Z" : ordinal === 195 ? "job-a" : "job-" + ordinal,
          );
        }),
      );
      const forwarded = all.find((item) => item.id === TurnItemId.make("job-3"));
      assert.equal(
        forwarded?.type === "system_notice" ? forwarded.job?.sourceThreadId : null,
        ThreadId.make("child"),
      );
      const detail = yield* service.getTurnItem({ threadId, itemId: TurnItemId.make("job-3") });
      assert.equal(
        detail.item?.type === "system_notice" ? detail.item.job?.output : null,
        "界".repeat(16_000),
      );
      // Ordinary transcript paging also keeps the old record available.
      const history = yield* projections.getThreadSnapshotWindow(threadId, {
        rowLimit: 25,
        anchorItemId: TurnItemId.make("job-3"),
      });
      assert.isTrue(
        history.projection.visibleTurnItems.some(
          (row) => row.sourceItemId === TurnItemId.make("job-3"),
        ),
      );
      assert.notInclude(JSON.stringify(history), "界");
    }).pipe(Effect.provide(ThreadManagementService.layer.pipe(Layer.provideMerge(layer)))),
);

it.effect(
  "completion between Stop planning and commit cannot resurrect a job, and the accepted intent still queues one source-bound effect",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const sink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("stop-race");
      const now = yield* DateTime.now;
      yield* createJobsThread(orchestrator, threadId);
      const active = jobFixture(threadId, 1, now);
      const providerThreadId = active.providerThreadId;
      if (providerThreadId === null) throw new Error("Active job fixture needs a provider thread");
      yield* sink.write({
        events: [
          {
            id: EventId.make("race-start"),
            type: "turn-item.updated",
            threadId,
            occurredAt: now,
            payload: active,
          },
        ],
      });
      const completed: OrchestrationV2TurnItem = {
        ...active,
        status: "completed",
        completedAt: now,
        job: {
          ...active.job!,
          state: "succeeded",
          exitCode: 0,
          endedAt: DateTime.toEpochMillis(now),
          output: "Final output",
        },
      };
      const commit = sink.commitCommand;
      // Inject direct provider ingestion at the existing sink boundary, after the
      // real orchestrator planned Stop but before its transaction commits.
      Object.assign(sink, {
        commitCommand: (input: Parameters<typeof commit>[0]) =>
          Effect.gen(function* () {
            if (input.commandType === "thread.job.stop") {
              yield* sink.write({
                events: [
                  {
                    id: EventId.make("race-completed"),
                    type: "turn-item.updated",
                    threadId,
                    occurredAt: now,
                    payload: completed,
                  },
                ],
              });
            }
            return yield* commit(input);
          }),
      });
      const command = {
        type: "thread.job.stop" as const,
        commandId: CommandId.make("race-stop"),
        threadId,
        turnItemId: active.id,
      };
      const result = yield* orchestrator.dispatch(command);
      assert.deepEqual(yield* projections.getTurnItem({ threadId, itemId: active.id }), completed);
      assert.isTrue(
        result.storedEvents.some(
          ({ event }) =>
            event.type === "turn-item.updated" &&
            event.payload.parentItemId === active.id &&
            event.payload.status === "completed",
        ),
      );
      const effects = yield* outbox.listByCommandId(command.commandId);
      assert.lengthOf(effects, 1);
      assert.equal(effects[0]?.threadId, ThreadId.make("child"));
      assert.deepEqual(effects[0]?.request, {
        type: "provider-job.stop",
        providerThreadId,
        providerSessionId: active.job!.providerSessionId,
        scope: active.job!.scope,
        jobId: active.job!.id,
      });
      yield* orchestrator.dispatch(command);
      assert.lengthOf(yield* outbox.listByCommandId(command.commandId), 1);
    }).pipe(Effect.provide(layer)),
);
