import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EventId,
  ThreadId,
  ProjectId,
  ProviderSessionId,
  ProviderInstanceId,
  RunId,
  RunAttemptId,
  NodeId,
  MessageId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as ServerConfig from "../../config.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as ProviderEventIngestor from "../ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "../ThreadCommandExecutor.ts";
import * as IdAllocator from "../IdAllocator.ts";
import { ProviderAdapterV2RuntimePolicy, type ProviderAdapterV2Event } from "../ProviderAdapter.ts";
import { makePiAdapterV2 } from "./PiAdapterV2.ts";

const database = SqlitePersistence.layerMemory;
const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(Layer.provide(database));
const sink = EventSink.layer.pipe(Layer.provide(Layer.merge(stores, database)));
const ingestor = ProviderEventIngestor.layer.pipe(
  Layer.provide(Layer.mergeAll(stores, sink, IdAllocator.layer, ThreadCommandExecutor.layer)),
);
const layer = Layer.mergeAll(
  NodeServices.layer,
  stores,
  sink,
  ingestor,
  IdAllocator.layer,
  ServerConfig.layerTest(process.cwd(), { prefix: "pi-jobs-integration-" }).pipe(
    Layer.provide(NodeServices.layer),
  ),
);
const extension = process.env.T3_PI_WAKE_EXTENSION;
const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

// Explicit opt-in: exercises a separately checked-out extension and an installed Pi,
// with isolated configuration/session files and no provider/model calls.
for (const { wakeOnExit, otherWork } of [
  { wakeOnExit: false, otherWork: "none" },
  { wakeOnExit: true, otherWork: "none" },
  { wakeOnExit: true, otherWork: "job" },
  { wakeOnExit: true, otherWork: "subagent" },
  { wakeOnExit: true, otherWork: "held-wake" },
]) {
  it.live.skipIf(!extension)(
    `real Pi jobs reconcile stop (wake_on_exit: ${wakeOnExit}, other work: ${otherWork})`,
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const dir = yield* fs.makeTempDirectoryScoped({ prefix: "pi-jobs-" });
        const driver = dir + "/driver.ts";
        yield* fs.writeFileString(
          driver,
          `import wake from ${encode(extension)};
export default function(pi) {
  const tools = new Map();
  wake(new Proxy(pi, { get(target, key) {
    if (key === "registerTool") return (tool) => { tools.set(tool.name, tool); target.registerTool(tool); };
    if (key === "sendUserMessage" || key === "sendMessage") return () => {};
    return target[key];
  }}));
  pi.registerCommand("exercise", { description: "Isolated test", handler: async (args, ctx) => {
    const job = JSON.parse(args);
    await tools.get("job_run").execute("test", job, undefined, undefined, ctx);
    const other = ${encode(otherWork)};
    if (other === "job") await tools.get("job_run").execute("other", { ...job, name: "Other job" }, undefined, undefined, ctx);
    if (other === "subagent") globalThis[Symbol.for("pi-subagents/runtime")] = { runningSubagents: new Map([["child", {}]]) };
    if (other === "held-wake") globalThis[Symbol.for("pi-subagents/keepalive")].add("wake:held");
  }});
}`,
        );
        const ids = yield* IdAllocator.IdAllocatorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const eventsIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("isolated-pi-job");
        const sessionId = ProviderSessionId.make("isolated-pi-session");
        const instanceId = ProviderInstanceId.make("pi");
        const modelSelection = { instanceId, model: "default" };
        const thread: OrchestrationV2AppThread = {
          id: threadId,
          projectId: ProjectId.make("isolated-project"),
          title: "Jobs integration",
          providerInstanceId: instanceId,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          createdBy: "user",
          creationSource: "web",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        };
        yield* eventSink.write({
          events: [
            {
              id: EventId.make("integration-thread"),
              type: "thread.created",
              threadId,
              occurredAt: now,
              payload: thread,
            },
          ],
        });
        const adapter = makePiAdapterV2({
          instanceId,
          settings: {
            enabled: true,
            binaryPath: process.env.PI_TEST_BINARY || "pi",
            launchArgs: `--no-extensions --no-skills --no-prompt-templates --no-context-files --session-dir ${encode(dir + "/sessions")} --extension ${encode(driver)}`,
            customModels: [],
          },
          environment: { ...process.env, PI_CODING_AGENT_DIR: dir + "/agent", TMPDIR: dir },
          spawner: yield* ChildProcessSpawner.ChildProcessSpawner,
          fileSystem: fs,
          idAllocator: ids,
          serverConfig: yield* ServerConfig.ServerConfig,
        });
        const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: dir,
        });
        const runtime = yield* adapter.openSession({
          threadId,
          providerSessionId: sessionId,
          modelSelection,
          runtimePolicy,
        });
        const queue = yield* Queue.unbounded<ProviderAdapterV2Event>();
        const terminal = yield* Deferred.make<void>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              if (event.type === "turn_item.updated" || event.type === "provider_thread.updated")
                yield* eventsIngestor.ingestNormalized({
                  providerSessionId: sessionId,
                  providerInstanceId: instanceId,
                  threadId,
                  event,
                });
              if (event.type === "turn.terminal") yield* Deferred.succeed(terminal, undefined);
              yield* Queue.offer(queue, event);
            }),
          ),
          Effect.forkScoped,
        );
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection,
          runtimePolicy,
        });
        const runId = RunId.make("isolated-run");
        yield* runtime.startTurn({
          appThread: thread,
          threadId,
          providerThread,
          runId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("isolated-attempt"),
          rootNodeId: NodeId.make("isolated-node"),
          modelSelection,
          runtimePolicy,
          message: {
            messageId: MessageId.make("isolated-message"),
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            text:
              "/exercise " +
              encode({
                name: "Real background job",
                command: `${encode(process.execPath)} -e 'console.log("ready"); setInterval(() => {}, 1000)'`,
                wake_on_exit: wakeOnExit,
              }),
          },
        });
        const takeJob = (state: string, id?: string) =>
          Effect.gen(function* () {
            while (true) {
              const event = yield* Queue.take(queue);
              if (
                event.type === "turn_item.updated" &&
                event.turnItem.type === "system_notice" &&
                event.turnItem.job?.state === state &&
                (id === undefined || event.turnItem.job.id === id) &&
                event.turnItem.job.output.includes("ready")
              )
                return event.turnItem;
            }
          });
        const running = yield* takeJob("running");
        yield* Deferred.await(terminal);
        const persisted = yield* projections.getThreadSnapshotWindow(threadId, { rowLimit: 1 });
        assert.equal(
          persisted.projection.providerThreads.some(
            (thread) => (thread.pendingBackgroundTasks?.length ?? 0) > 0,
          ),
          wakeOnExit,
        );
        const saved = persisted.projection.turnItems.find((item) => item.id === running.id);
        assert.isTrue(saved?.type === "system_notice" && saved.job?.output.includes("ready"));
        yield* runtime.stopJob!({
          providerThread,
          scope: running.job!.scope,
          jobId: running.job!.id,
        });
        const stopped = yield* takeJob("stopped", running.job!.id);
        assert.equal(stopped.id, running.id);
        assert.equal(stopped.job?.signal, "SIGTERM");
        const restored = yield* projections.getThreadSnapshotWindow(threadId, { rowLimit: 1 });
        assert.equal(
          (restored.projection.providerThreads[0]?.pendingBackgroundTasks?.length ?? 0) > 0,
          otherWork !== "none",
        );
        assert.isTrue(
          restored.projection.turnItems.some(
            (item) => item.type === "system_notice" && item.job?.state === "stopped",
          ),
        );
      }).pipe(Effect.scoped, Effect.provide(layer)),
    30000,
  );
}
