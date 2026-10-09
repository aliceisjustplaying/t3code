import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import { PI_PROVIDER } from "@t3tools/provider-pi/server";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/continuationRequests";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2RollbackThreadInput,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as ServerConfig from "../../config.ts";
import * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as ProviderEventIngestor from "../ProviderEventIngestor.ts";
import * as ProviderSessionManager from "../ProviderSessionManager.ts";
import * as ThreadCommandExecutor from "../ThreadCommandExecutor.ts";
import * as PiTestkit from "./PiAdapterV2.testkit.ts";

const PI_INSTANCE_ID = ProviderInstanceId.make("pi");
const THREAD_ID = ThreadId.make("thread-pi-managed-veto");
const SESSION_ID = ProviderSessionId.make("provider-session-pi-managed-veto");
const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});
const modelSelection = (model: string): ModelSelection => ({ instanceId: PI_INSTANCE_ID, model });
const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  ServerConfig.layerTest(process.cwd(), { prefix: "t3-pi-managed-veto-" }).pipe(
    Layer.provide(NodeServices.layer),
  ),
);

// Only the process boundary is fake. The public Pi driver, RPC framing and
// server session manager run unchanged; the fork response is explicitly gated.
const makeVetoingPi = Effect.gen(function* () {
  const stdout = yield* Queue.unbounded<Uint8Array>();
  const forkRequest = yield* Deferred.make<Record<string, unknown>>();
  const requests: Array<Record<string, unknown>> = [];
  const decode = Schema.decodeEffect(
    Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
  );
  const emit = (frame: Record<string, unknown>) =>
    Queue.offer(stdout, new TextEncoder().encode(JSON.stringify(frame) + "\n")).pipe(Effect.asVoid);
  let buffer = "";
  const decoder = new TextDecoder();
  const spawner = ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(999_999_999),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.gen(function* () {
            buffer += decoder.decode(chunk, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop()!;
            for (const line of lines.filter(Boolean)) {
              const request = yield* decode(line).pipe(Effect.orDie);
              requests.push(request);
              if (request.type === "fork") {
                yield* Deferred.succeed(forkRequest, request);
                continue;
              }
              if (
                request.type === "prompt" &&
                String(request.message).startsWith("/t3-background-work ")
              ) {
                yield* emit({
                  type: "extension_ui_request",
                  method: "setStatus",
                  statusKey: "t3:background-work",
                  statusText: JSON.stringify({
                    probeId: String(request.message).split(" ")[1],
                    pending: false,
                    retained: false,
                  }),
                });
              }
              const data =
                request.type === "get_state"
                  ? {
                      thinkingLevel: "high",
                      isStreaming: false,
                      isCompacting: false,
                      steeringMode: "one-at-a-time",
                      followUpMode: "one-at-a-time",
                      sessionFile: "/fake/pi-session.jsonl",
                      sessionId: "00000000-0000-4000-8000-000000000002",
                      autoCompactionEnabled: true,
                      messageCount: 0,
                      pendingMessageCount: 0,
                    }
                  : request.type === "get_commands"
                    ? {
                        commands: [{ name: "t3-background-work", source: "extension" }],
                      }
                    : request.type === "get_available_models"
                      ? { models: [] }
                      : request.type === "get_entries"
                        ? { entries: [], leafId: null }
                        : request.type === "get_messages"
                          ? { messages: [] }
                          : {};
              yield* emit({
                type: "response",
                id: request.id,
                command: request.type,
                success: true,
                data,
              });
            }
          }),
        ),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    ),
  );
  return { spawner, emit, forkRequest, requests };
});

const openManagedRuntime = Effect.fnUntraced(function* (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  continuations: typeof ProviderContinuationRequests.ProviderContinuationRequests.Service,
) {
  const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
    Layer.provide(SqlitePersistence.layerMemory),
  );
  const sink = EventSink.layer.pipe(
    Layer.provide(Layer.mergeAll(stores, SqlitePersistence.layerMemory)),
  );
  const ingestor = ProviderEventIngestor.layer.pipe(
    Layer.provide(Layer.mergeAll(stores, sink, IdAllocator.layer, ThreadCommandExecutor.layer)),
  );
  const registry = PiTestkit.layer({
    scenario: "managed rollback veto",
    binaryPath: "pi",
    launchArgs: "",
    spawner: Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
  }).pipe(
    Layer.provide(
      Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, continuations),
    ),
  );
  const managerLayer = ProviderSessionManager.layerWithOptions({ configureMcp: false }).pipe(
    Layer.provide(
      Layer.mergeAll(
        stores,
        sink,
        ingestor,
        IdAllocator.layer,
        registry,
        Layer.mock(McpSessionRegistry.McpSessionRegistry)({}),
      ),
    ),
  );
  const services = yield* Layer.build(Layer.merge(managerLayer, sink));
  const runtime = yield* Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const thread = yield* makeAppThread("default");
    yield* eventSink.write({
      events: [
        {
          id: yield* ids.allocate.event({ threadId: THREAD_ID }),
          type: "thread.created",
          threadId: THREAD_ID,
          occurredAt: thread.createdAt,
          payload: thread,
        },
      ],
    });
    const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
    return yield* manager.open({
      threadId: THREAD_ID,
      providerSessionId: SESSION_ID,
      modelSelection: modelSelection("default"),
      runtimePolicy,
    });
  }).pipe(Effect.provideContext(services));
  const emitted = yield* Queue.unbounded<ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  return { runtime, takeEvent: () => Queue.take(emitted) };
});

const makeAppThread = Effect.fnUntraced(function* (model: string, threadId = THREAD_ID) {
  const now = yield* DateTime.now;
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: "project:fixture:pi" as OrchestrationV2AppThread["projectId"],
    title: "Pi test thread",
    providerInstanceId: PI_INSTANCE_ID,
    modelSelection: modelSelection(model),
    runtimeMode: "full-access",
    interactionMode: "default",
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
  } satisfies OrchestrationV2AppThread;
});

const rollbackInput = (providerThread: OrchestrationV2ProviderThread) =>
  ({
    providerThread,
    target: {
      type: "thread_start",
      checkpointId: CheckpointId.make("checkpoint-pi"),
      appRunOrdinal: 0,
    },
    providerThreadTurns: [
      {
        id: ProviderTurnId.make("discarded-pi-turn"),
        providerThreadId: providerThread.id,
        nodeId: NodeId.make("discarded-pi-node"),
        runAttemptId: null,
        nativeTurnRef: {
          driver: PI_PROVIDER,
          nativeId: "discarded-user-entry",
          strength: "strong",
        },
        ordinal: 1,
        status: "completed",
        startedAt: null,
        completedAt: null,
      },
    ],
  }) satisfies ProviderAdapterV2RollbackThreadInput;

describe("Pi managed resume", () => {
  it.effect.each(["already held", "arriving during fork"] as const)(
    "managed resume preserves an unsolicited run %s after an explicit rollback veto",
    (arrival) =>
      Effect.gen(function* () {
        const fake = yield* makeVetoingPi;
        const offered =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const offers: ProviderContinuationRequests.ProviderContinuationRequest[] = [];
        const { runtime, takeEvent } = yield* openManagedRuntime(fake.spawner, {
          offer: (request) =>
            Effect.gen(function* () {
              offers.push(request);
              yield* Queue.offer(offered, request);
            }),
          take: Queue.take(offered),
        });
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        let originalOffer: ProviderContinuationRequests.ProviderContinuationRequest | undefined;
        if (arrival === "already held") {
          yield* fake.emit({ type: "agent_start" });
          originalOffer = yield* Queue.take(offered);
          yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
          yield* fake.emit({
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Before fork. " },
          });
        }
        const rollback = yield* runtime
          .rollbackThread(rollbackInput(providerThread))
          .pipe(Effect.flip, Effect.forkChild);
        const fork = yield* Deferred.await(fake.forkRequest);
        if (arrival === "arriving during fork") {
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
        }
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "During fork. " },
        });
        yield* fake.emit({
          type: "response",
          id: fork.id,
          command: "fork",
          success: true,
          data: { cancelled: true },
        });
        const error = yield* Fiber.join(rollback);
        assert.equal(error._tag, "ProviderAdapterRollbackThreadError");
        assert.isTrue(
          error._tag === "ProviderAdapterRollbackThreadError" && error.nativeSessionUnchanged,
        );
        // The veto leaves Pi's original runtime streaming, not replaced.
        assert.equal(offers.length, arrival === "already held" ? 2 : 1);
        assert.equal(runtime.providerSession.status, "ready");
        if (originalOffer !== undefined) {
          assert.deepEqual(
            yield* originalOffer.dispatchIfCurrent!(Effect.succeed("stale")),
            Option.none(),
          );
          yield* originalOffer.clearIfCurrent!();
        }
        const reoffered = offers.at(-1)!;
        assert.equal(reoffered.providerThreadId, providerThread.id);
        assert.deepEqual(
          yield* reoffered.dispatchIfCurrent!(Effect.succeed("continue")),
          Option.some("continue"),
        );
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "After veto. " },
        });
        const appThread = yield* makeAppThread("default");
        const runId = RunId.make(`run:${THREAD_ID}:2`);
        // Continuation startup resumes before adopting the restored wake.
        yield* runtime.resumeThread({
          threadId: THREAD_ID,
          providerThread,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* runtime.startTurn({
          appThread,
          threadId: THREAD_ID,
          runId,
          runOrdinal: 2,
          providerTurnOrdinal: 2,
          attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
          rootNodeId: NodeId.make(`node:${runId}:root`),
          providerThread,
          message: {
            messageId: `message:${THREAD_ID}:2` as never,
            text: "Background task completed.",
            attachments: [],
            createdBy: "agent",
            creationSource: "provider",
          },
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Continued." },
        });
        yield* fake.emit({ type: "message_end", message: { role: "assistant", content: [] } });
        yield* fake.emit({ type: "agent_settled" });
        const texts: string[] = [];
        while (true) {
          const event = yield* takeEvent();
          if (event.type === "turn_item.updated" && event.turnItem.type === "assistant_message") {
            assert.equal(event.turnItem.runId, runId);
            texts.push(event.turnItem.text);
          }
          if (event.type === "turn.terminal") {
            assert.equal(event.status, "completed");
            break;
          }
        }
        assert.include(
          texts,
          `${arrival === "already held" ? "Before fork. " : ""}During fork. After veto. Continued.`,
        );
        assert.isFalse(
          fake.requests.some(
            (request) =>
              request.type === "abort" ||
              request.type === "switch_session" ||
              (request.type === "prompt" &&
                !String(request.message).startsWith("/t3-background-work ")),
          ),
        );
        assert.deepEqual(
          yield* reoffered.dispatchIfCurrent!(Effect.succeed("again")),
          Option.none(),
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
