import { assert, describe, it, vi } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointId,
  EnvironmentId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as ServerConfig from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as IdAllocator from "../IdAllocator.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as ProviderEventIngestor from "../ProviderEventIngestor.ts";
import * as ProviderSessionManager from "../ProviderSessionManager.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import * as ThreadCommandExecutor from "../ThreadCommandExecutor.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2RollbackThreadInput,
  type ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import { handoffBudget } from "../ContextHandoffBudget.ts";
import type * as ProviderContinuationRequests from "../ProviderContinuationRequests.ts";
import { makePiAdapterV2, PI_PROVIDER } from "./PiAdapterV2.ts";
import { makePiRpcConnection, type PiRpcRecord } from "./PiRpc.ts";
import * as PiRpc from "./PiRpc.ts";

const layerServerConfig = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-pi-v2-adapter-",
}).pipe(Layer.provide(NodeServices.layer));

const layerTest = Layer.mergeAll(NodeServices.layer, IdAllocator.layer, layerServerConfig);

const decodeJsonLine = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const PI_INSTANCE_ID = ProviderInstanceId.make("pi");
const THREAD_ID = ThreadId.make("thread-pi-test");
const SESSION_ID = ProviderSessionId.make("provider-session-pi-test");
const FAKE_SESSION_FILE = "/fake/.pi/agent/sessions/--workspace--/0001_abc.jsonl";
/** Deliberately outside the valid pid range so a group-kill can never land. */
const FAKE_PID = 999_999_999;

const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});

const modelSelection = (model: string): ModelSelection => ({
  instanceId: PI_INSTANCE_ID,
  model,
});

interface FakePi {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly emit: (record: PiRpcRecord) => Effect.Effect<void>;
  readonly takeRequest: (type: string) => Effect.Effect<PiRpcRecord>;
  /** Data returned by the next `get_entries` acks, consumed in order. */
  readonly queueEntries: (data: unknown) => void;
  /** Data returned by the next active-branch `get_messages` acks. */
  readonly queueMessages: (data: unknown) => void;
  /** Make the next `switch_session` ack report an extension veto. */
  readonly vetoNextSwitch: () => void;
  /** Fields overriding the recorded idle state in the next `get_state` acks, in order. */
  readonly queueState: (data: Record<string, unknown>) => void;
  /** Hold the next `get_state` response until the test resolves it. */
  readonly deferNextState: () => void;
  /** Resolve the held `get_state` request. */
  readonly resolveDeferredState: (data: unknown) => Effect.Effect<void>;
  /** Reject the next `count` state requests, defaulting to one. */
  readonly failNextState: (count?: number) => void;
  readonly failedStateReads: () => number;
  readonly deferNextRequest: (type: "switch_session" | "new_session" | "fork" | "prompt") => void;
  readonly queueModels: (models: ReadonlyArray<unknown>) => void;
  readonly vetoNextNewSession: () => void;
  /** Every request received by the fake process. */
  readonly allRequests: () => ReadonlyArray<PiRpcRecord>;
  /** Data returned by the next `get_session_stats` acks, consumed in order. */
  readonly queueStats: (data: unknown) => void;
  /** Data returned by the next `get_commands` acks, consumed in order. */
  readonly queueCommands: (data: unknown) => void;
  /** Make the next `get_commands` ack fail. */
  readonly failNextCommands: () => void;
  /** Close the fake process stdout stream. */
  readonly closeStdout: Effect.Effect<void>;
  readonly setBackgroundWork: (pending: boolean, retained?: boolean) => void;
  readonly lastSpawn: () => {
    readonly args: ReadonlyArray<string>;
    readonly env: NodeJS.ProcessEnv;
  };
}

/**
 * Pi 1.0.0's idle `get_state` reply, taken from the `simple` replay fixture
 * (fixtures/simple/pi_transcript.ndjson) minus the model object. Pi omits
 * `model` when none is selected and `sessionName` until one is set.
 */
const recordedIdleState = (sessionFile: string) => ({
  thinkingLevel: "high",
  isStreaming: false,
  isCompacting: false,
  steeringMode: "one-at-a-time",
  followUpMode: "one-at-a-time",
  sessionFile,
  sessionId: "00000000-0000-4000-8000-000000000002",
  autoCompactionEnabled: true,
  messageCount: 0,
  pendingMessageCount: 0,
});

/**
 * In-process fake `pi --mode rpc` for races and failures a live Pi cannot
 * produce on demand: captures every stdin record, auto-acks requests, and lets
 * tests push protocol events to stdout. Behaviour a real Pi can show belongs
 * in a replay fixture instead (see PiAdapterV2.testkit.ts).
 */
const makeFakePi: Effect.Effect<FakePi> = Effect.gen(function* () {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const requests = yield* Queue.unbounded<PiRpcRecord>();
  const entriesQueue: Array<unknown> = [];
  const messagesQueue: Array<unknown> = [];
  const stateQueue: Array<Record<string, unknown>> = [];
  const statsQueue: Array<unknown> = [];
  const commandsQueue: Array<{ readonly success: boolean; readonly data?: unknown }> = [];
  const allRequests: Array<PiRpcRecord> = [];
  let backgroundWork: string = "idle";
  let deferState = false;
  let deferredStateRequest: PiRpcRecord | undefined;
  let stateFailuresRemaining = 0;
  let failedStateReads = 0;
  let vetoSwitch = false;
  let vetoNewSession = false;
  let deferredLifecycle: string | undefined;
  let sessionFile = FAKE_SESSION_FILE;
  let sessionGeneration = 0;
  let models: ReadonlyArray<unknown> = [];
  let stdinBuffer = "";

  const emit = (record: PiRpcRecord) =>
    Queue.offer(stdout, new TextEncoder().encode(`${encodeJsonLine(record)}\n`)).pipe(
      Effect.asVoid,
    );

  const respondTo = (record: PiRpcRecord): PiRpcRecord | null => {
    if (typeof record["id"] !== "string") return null;
    const base = {
      type: "response",
      id: record["id"],
      command: String(record["type"]),
      success: true,
    };
    switch (record["type"]) {
      case "get_state":
        if (stateFailuresRemaining > 0) {
          stateFailuresRemaining--;
          failedStateReads++;
          return { ...base, success: false, error: "state unavailable" };
        }
        // Queued data overrides fields of the recorded idle state, so a test
        // that only cares about the session file still gets a real shape.
        return { ...base, data: { ...recordedIdleState(sessionFile), ...stateQueue.shift() } };
      case "get_available_models":
        return { ...base, data: { models } };
      case "new_session": {
        const cancelled = vetoNewSession;
        vetoNewSession = false;
        if (!cancelled) sessionFile = `/fake/new-${++sessionGeneration}.jsonl`;
        return { ...base, data: { cancelled } };
      }
      case "switch_session": {
        const cancelled = vetoSwitch;
        vetoSwitch = false;
        return { ...base, data: { cancelled } };
      }
      case "get_entries":
        return { ...base, data: entriesQueue.shift() ?? { entries: [], leafId: null } };
      case "get_messages":
        return { ...base, data: messagesQueue.shift() ?? { messages: [] } };
      case "get_session_stats":
        return { ...base, data: statsQueue.shift() ?? {} };
      case "get_commands":
        return {
          ...base,
          ...(commandsQueue.shift() ?? {
            data: { commands: [{ name: "t3-background-work", source: "extension" }] },
          }),
        };
      case "fork":
        sessionFile = `/fake/fork-${++sessionGeneration}.jsonl`;
        return { ...base, data: { text: "Hello pi", cancelled: false } };
      default:
        return base;
    }
  };

  const handleStdinChunk = (chunk: Uint8Array) =>
    Effect.gen(function* () {
      stdinBuffer += new TextDecoder().decode(chunk);
      while (true) {
        const newline = stdinBuffer.indexOf("\n");
        if (newline === -1) return;
        const line = stdinBuffer.slice(0, newline);
        stdinBuffer = stdinBuffer.slice(newline + 1);
        if (line.length === 0) continue;
        const record = decodeJsonLine(line) as PiRpcRecord;
        allRequests.push(record);
        yield* Queue.offer(requests, record);
        if (record["type"] === "get_state" && deferState) {
          deferState = false;
          deferredStateRequest = record;
          continue;
        }
        if (record["type"] === deferredLifecycle) {
          deferredLifecycle = undefined;
          continue;
        }
        if (record["type"] === "prompt" && record["message"] === "/t3-background-work") {
          yield* emit({
            type: "extension_ui_request",
            method: "setStatus",
            statusKey: "t3:background-work",
            statusText: backgroundWork,
          });
        }
        const response = respondTo(record);
        if (response !== null) yield* emit(response);
      }
    });

  let lastSpawn: { readonly args: ReadonlyArray<string>; readonly env: NodeJS.ProcessEnv } = {
    args: [],
    env: {},
  };
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.sync(() => {
      if (ChildProcess.isStandardCommand(command)) {
        lastSpawn = {
          args: command.args,
          env: command.options.env ?? {},
        };
      }
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(FAKE_PID),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach(handleStdinChunk),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );

  const takeRequest = (type: string): Effect.Effect<PiRpcRecord> =>
    Effect.gen(function* () {
      while (true) {
        const record = yield* Queue.take(requests);
        if (record["type"] === type) return record;
      }
    });

  return {
    spawner,
    emit,
    takeRequest,
    queueEntries: (data) => entriesQueue.push(data),
    queueMessages: (data) => messagesQueue.push(data),
    deferNextState: () => {
      deferState = true;
    },
    resolveDeferredState: (data) =>
      Effect.gen(function* () {
        const record = deferredStateRequest;
        assert.isDefined(record);
        deferredStateRequest = undefined;
        yield* emit({
          type: "response",
          id: record!["id"],
          command: "get_state",
          success: true,
          data,
        });
      }),
    failNextState: (count = 1) => {
      stateFailuresRemaining = count;
    },
    failedStateReads: () => failedStateReads,
    deferNextRequest: (type) => {
      deferredLifecycle = type;
    },
    queueModels: (value) => {
      models = value;
    },
    vetoNextNewSession: () => {
      vetoNewSession = true;
    },
    allRequests: () => allRequests,
    vetoNextSwitch: () => {
      vetoSwitch = true;
    },
    queueState: (data) => stateQueue.push(data),
    queueStats: (data) => statsQueue.push(data),
    queueCommands: (data) => commandsQueue.push({ success: true, data }),
    failNextCommands: () => commandsQueue.push({ success: false }),
    setBackgroundWork: (pending, retained) => {
      backgroundWork =
        retained === undefined
          ? pending
            ? "pending"
            : "idle"
          : JSON.stringify({ pending, retained });
    },
    closeStdout: Queue.end(stdout),
    lastSpawn: () => lastSpawn,
  } satisfies FakePi;
});

const makeAdapter = Effect.fnUntraced(function* (
  fake: FakePi,
  launchArgs = "",
  forkFake?: FakePi,
  continuationRequests?: Parameters<typeof makePiAdapterV2>[0]["continuationRequests"],
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  return makePiAdapterV2({
    instanceId: PI_INSTANCE_ID,
    settings: { enabled: true, binaryPath: "pi", launchArgs, customModels: [] },
    environment: {},
    spawner:
      forkFake === undefined
        ? fake.spawner
        : ChildProcessSpawner.make((command) =>
            ChildProcess.isStandardCommand(command) && command.args.includes("--fork")
              ? forkFake.spawner.spawn(command)
              : fake.spawner.spawn(command),
          ),
    fileSystem,
    idAllocator,
    serverConfig,
    ...(continuationRequests === undefined ? {} : { continuationRequests }),
  });
});

const openRuntime = Effect.fnUntraced(function* (
  fake: FakePi,
  model = "default",
  threadId = THREAD_ID,
  providerSessionId = SESSION_ID,
  forkFake?: FakePi,
  continuationRequests?: Parameters<typeof makePiAdapterV2>[0]["continuationRequests"],
  managed = false,
) {
  const adapter = yield* makeAdapter(fake, "", forkFake, continuationRequests);
  const input = {
    threadId,
    providerSessionId,
    modelSelection: modelSelection(model),
    runtimePolicy,
  };
  let runtime: ProviderAdapterV2SessionRuntime;
  if (managed) {
    const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
      Layer.provide(SqlitePersistence.layerMemory),
    );
    const sink = EventSink.layer.pipe(
      Layer.provide(Layer.mergeAll(stores, SqlitePersistence.layerMemory)),
    );
    const ingestor = ProviderEventIngestor.layer.pipe(
      Layer.provide(Layer.mergeAll(stores, sink, IdAllocator.layer, ThreadCommandExecutor.layer)),
    );
    const managerLayer = ProviderSessionManager.layerWithOptions({ configureMcp: false }).pipe(
      Layer.provide(
        Layer.mergeAll(
          stores,
          sink,
          ingestor,
          IdAllocator.layer,
          ProviderAdapterRegistry.layerSingle(adapter),
          Layer.mock(McpSessionRegistry.McpSessionRegistry)({}),
        ),
      ),
    );
    const services = yield* Layer.build(Layer.merge(managerLayer, sink));
    runtime = yield* Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const thread = yield* makeAppThread(model, threadId);
      yield* eventSink.write({
        events: [
          {
            id: yield* ids.allocate.event({ threadId }),
            type: "thread.created",
            threadId,
            occurredAt: thread.createdAt,
            payload: thread,
          },
        ],
      });
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      return yield* manager.open(input);
    }).pipe(Effect.provideContext(services));
  } else {
    runtime = yield* adapter.openSession(input);
  }
  const emitted = yield* Queue.unbounded<ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  const takeEvent = (predicate: (event: ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(emitted);
        if (predicate(event)) return event;
      }
    });
  return { runtime, takeEvent };
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

const startTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  model = "default",
  attachments: ReadonlyArray<ChatAttachment> = [],
  text = "Hello pi",
  selection?: ModelSelection,
  runOrdinal = 1,
  threadId = THREAD_ID,
) {
  const appThread = yield* makeAppThread(model, threadId);
  const runId = RunId.make(`run:${threadId}:${runOrdinal}`);
  yield* runtime.startTurn({
    appThread,
    threadId,
    runId,
    runOrdinal,
    providerTurnOrdinal: runOrdinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${threadId}:${runOrdinal}` as never,
      text,
      attachments,
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: selection ?? modelSelection(model),
    runtimePolicy,
  });
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

const expectModelFailure = (errorMessage: string) =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    const { runtime, takeEvent } = yield* openRuntime(fake);
    const providerThread = yield* runtime.ensureThread({
      threadId: THREAD_ID,
      modelSelection: modelSelection("default"),
      runtimePolicy,
    });
    yield* startTurn(runtime, providerThread);
    yield* fake.takeRequest("prompt");
    yield* fake.emit({ type: "agent_start" });
    yield* fake.emit({
      type: "message_end",
      message: {
        role: "assistant",
        content: [],
        stopReason: "error",
        errorMessage,
      },
    });
    yield* fake.emit({ type: "agent_settled" });

    const sessionError = yield* takeEvent(
      (event) =>
        event.type === "provider_session.updated" && event.providerSession.status === "error",
    );
    assert.isTrue(
      sessionError.type === "provider_session.updated" &&
        sessionError.providerSession.lastError === errorMessage,
    );
    const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
    assert.isTrue(
      terminal.type === "turn.terminal" &&
        terminal.status === "failed" &&
        terminal.failure.message === errorMessage,
    );
  }).pipe(Effect.scoped, Effect.provide(layerTest));

describe("PiAdapterV2", () => {
  it.effect.each(["rollback", "resume", "new_session"] as const)(
    "drops a held wake and its saved callbacks when %s replaces the native session",
    (operation) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        let providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        const offer = yield* Queue.take(offers);
        let dispatched = false;
        // Construct before replacement; validity is checked when this effect runs.
        const savedDispatch = offer.dispatchIfCurrent!(
          Effect.sync(() => {
            dispatched = true;
          }),
        );
        yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: 0,
            delta: "Discarded output.",
          },
        });
        yield* fake.emit({ type: "message_end", message: { role: "assistant", content: [] } });
        yield* fake.emit({ type: "agent_settled" });
        if (operation === "rollback") {
          providerThread = (yield* runtime.rollbackThread(rollbackInput(providerThread)))
            .providerThread;
          assert.match(providerThread.nativeThreadRef!.nativeId!, /fork-/);
        } else if (operation === "resume") {
          providerThread = yield* runtime.resumeThread({ providerThread });
        } else {
          providerThread = yield* runtime.ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
          });
        }
        assert.deepEqual(yield* savedDispatch, Option.none());
        assert.isFalse(dispatched);
        yield* offer.clearIfCurrent!();
        assert.isFalse(fake.allRequests().some((request) => request.type === "abort"));
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({
          type: "response",
          command: "prompt",
          success: true,
          data: { disposition: "started" },
        });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Fresh output." },
        });
        yield* fake.emit({ type: "message_end", message: { role: "assistant", content: [] } });
        yield* fake.emit({ type: "agent_settled" });
        const texts: string[] = [];
        while (true) {
          const event = yield* takeEvent(() => true);
          if (event.type === "turn_item.updated" && event.turnItem.type === "assistant_message")
            texts.push(event.turnItem.text);
          if (event.type === "turn.terminal") {
            assert.equal(event.status, "completed");
            break;
          }
        }
        assert.isTrue(texts.some((text) => text === "Fresh output."));
        assert.isFalse(texts.some((text) => text.includes("Discarded output.")));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("does not offer or replay a wake arriving while native rollback is running", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const offers: ProviderContinuationRequests.ProviderContinuationRequest[] = [];
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        {
          offer: (request) =>
            Effect.sync(() => {
              offers.push(request);
            }),
        },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextRequest("fork");
      const rollback = yield* Effect.forkChild(
        runtime.rollbackThread(rollbackInput(providerThread)),
      );
      const fork = yield* fake.takeRequest("fork");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Discarded output." },
      });
      yield* fake.emit({ type: "message_end", message: { role: "assistant", content: [] } });
      yield* fake.emit({ type: "agent_settled" });
      fake.queueState({ sessionFile: "/fake/rolled-back.jsonl" });
      yield* fake.emit({
        type: "response",
        id: fork.id,
        command: "fork",
        success: true,
        data: { cancelled: false },
      });
      const rolledBack = (yield* Fiber.join(rollback)).providerThread;
      assert.equal(rolledBack.nativeThreadRef?.nativeId, "/fake/rolled-back.jsonl");
      yield* startTurn(runtime, rolledBack);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({
        type: "response",
        command: "prompt",
        success: false,
        error: "new prompt rejected",
      });
      const texts: string[] = [];
      while (true) {
        const event = yield* takeEvent(() => true);
        if (event.type === "turn_item.updated" && event.turnItem.type === "assistant_message")
          texts.push(event.turnItem.text);
        if (event.type === "turn.terminal") {
          assert.equal(event.status, "failed");
          break;
        }
      }
      assert.deepEqual(texts, []);
      assert.equal(offers.length, 0);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["already held", "arriving during fork"] as const)(
    "managed resume preserves an unsolicited run %s after an explicit rollback veto",
    (arrival) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offered =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const offers: ProviderContinuationRequests.ProviderContinuationRequest[] = [];
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          {
            offer: (request) =>
              Effect.gen(function* () {
                offers.push(request);
                yield* Queue.offer(offered, request);
              }),
          },
          true,
        );
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
        fake.deferNextRequest("fork");
        const rollback = yield* runtime
          .rollbackThread(rollbackInput(providerThread))
          .pipe(Effect.flip, Effect.forkChild);
        const fork = yield* fake.takeRequest("fork");
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
          const event = yield* takeEvent(() => true);
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
          fake
            .allRequests()
            .some(
              (request) =>
                request.type === "abort" ||
                request.type === "switch_session" ||
                (request.type === "prompt" && request.message !== "/t3-background-work"),
            ),
        );
        assert.deepEqual(
          yield* reoffered.dispatchIfCurrent!(Effect.succeed("again")),
          Option.none(),
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["RPC error", "interruption"] as const)(
    "retires quarantined work after rollback ends with %s",
    (failure) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          { offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid) },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        const offer = yield* Queue.take(offers);
        fake.deferNextRequest("fork");
        const rollback = yield* runtime
          .rollbackThread(rollbackInput(providerThread))
          .pipe(Effect.exit, Effect.forkChild);
        const fork = yield* fake.takeRequest("fork");
        if (failure === "RPC error") {
          yield* fake.emit({
            type: "response",
            id: fork.id,
            command: "fork",
            success: false,
            error: "Replacement runtime failed to initialize",
          });
          assert.isTrue(Exit.isFailure(yield* Fiber.join(rollback)));
        } else {
          yield* Fiber.interrupt(rollback);
        }
        yield* takeEvent(
          (event) =>
            event.type === "provider_session.updated" && event.providerSession.status === "error",
        );
        assert.deepEqual(yield* offer.dispatchIfCurrent!(Effect.succeed("stale")), Option.none());
        yield* offer.clearIfCurrent!();
        yield* startTurn(runtime, providerThread).pipe(Effect.flip);
        assert.isFalse(
          fake
            .allRequests()
            .some((request) => request.type === "abort" || request.type === "prompt"),
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("invalidates saved wake dispatch and clear callbacks when the session closes", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const sessionScope = yield* Scope.make();
      const { runtime } = yield* openRuntime(fake, "default", THREAD_ID, SESSION_ID, undefined, {
        offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
      }).pipe(Effect.provideService(Scope.Scope, sessionScope));
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      const offer = yield* Queue.take(offers);
      let dispatched = false;
      const savedDispatch = offer.dispatchIfCurrent!(
        Effect.sync(() => {
          dispatched = true;
        }),
      );
      yield* Scope.close(sessionScope, Exit.void);
      assert.deepEqual(yield* savedDispatch, Option.none());
      assert.isFalse(dispatched);
      yield* offer.clearIfCurrent!();
      assert.isFalse(fake.allRequests().some((request) => request.type === "abort"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { commands: [], label: "missing command" },
    { commands: [{ name: "t3-background-work", source: "prompt" }], label: "prompt template only" },
  ])("settles without prompting the model with $label", ({ commands }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.takeRequest("get_commands"); // Startup skill discovery has completed.
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      fake.queueCommands({ commands });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_settled" });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      assert.deepEqual(
        fake
          .allRequests()
          .filter((request) => request.type === "prompt")
          .map((request) => request.message),
        ["Hello pi"],
      );
      fake.queueCommands({ commands });
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      assert.equal(fake.allRequests().filter((request) => request.type === "prompt").length, 1);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps background work pinned without prompting when command discovery fails", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      yield* fake.takeRequest("get_commands");
      fake.failNextCommands();
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      assert.isFalse(fake.allRequests().some((request) => request.type === "prompt"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("hands a turn Pi starts on its own to the continuation turn it asks for", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      // An extension wakes the idle session (e.g. a background task finished).
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Task finished." },
      });
      yield* fake.emit({ type: "message_end", message: { role: "assistant", content: [] } });
      yield* fake.emit({ type: "agent_settled" });

      const offer = yield* Queue.take(offers);
      assert.equal(offer.threadId, THREAD_ID);
      assert.equal(offer.providerThreadId, providerThread.id);
      const dispatched = yield* offer.dispatchIfCurrent!(Effect.succeed("dispatched"));
      assert.deepEqual(dispatched, Option.some("dispatched"));

      const appThread = yield* makeAppThread("default");
      const runId = RunId.make(`run:${THREAD_ID}:2`);
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

      const reply = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "assistant_message",
      );
      assert.isTrue(
        reply.type === "turn_item.updated" &&
          reply.turnItem.type === "assistant_message" &&
          reply.turnItem.runId === runId &&
          reply.turnItem.text === "Task finished.",
      );
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      // Taking over the run must not send another model prompt; only the
      // extension keepalive query is allowed.
      assert.isFalse(
        fake
          .allRequests()
          .some((record) => record.type === "prompt" && record.message !== "/t3-background-work"),
      );
      // A taken wake asks for no second turn.
      assert.deepEqual(yield* offer.dispatchIfCurrent!(Effect.succeed("again")), Option.none());
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("lets a user turn that starts first take a held Pi run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
      const offer = yield* Queue.take(offers);

      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Both." },
      });
      yield* fake.emit({ type: "message_end", message: { role: "assistant", content: [] } });
      const reply = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "assistant_message",
      );
      assert.isTrue(
        reply.type === "turn_item.updated" &&
          reply.turnItem.runId === RunId.make(`run:${THREAD_ID}:1`),
      );
      assert.deepEqual(yield* offer.dispatchIfCurrent!(Effect.succeed("late")), Option.none());
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a taken Pi run in the user turn when Pi rejects that turn's prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        { offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid) },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      // An extension's run is still streaming when the user turn takes it.
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Still " },
      });
      yield* Queue.take(offers);

      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({
        type: "response",
        command: "prompt",
        success: false,
        error: "Agent is already processing.",
      });
      // The taken run goes on after the rejection.
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "going." },
      });
      yield* fake.emit({ type: "message_end", message: { role: "assistant", content: [] } });
      yield* fake.emit({ type: "agent_settled" });

      const runId = RunId.make(`run:${THREAD_ID}:1`);
      let replyText: string | null = null;
      const terminal = yield* takeEvent((event) => {
        if (
          event.type === "turn_item.updated" &&
          event.turnItem.type === "assistant_message" &&
          event.turnItem.runId === runId
        ) {
          replyText = event.turnItem.text;
        }
        return event.type === "turn.terminal";
      });
      // Finalizing at the rejection would have cut the reply at "Still ".
      assert.equal(replyText, "Still going.");
      assert.isTrue(
        terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message === "Agent is already processing.",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "does not settle a user turn on a taken Pi run's settle before Pi acks the prompt",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          { offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid) },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        // An extension's run has finished, but its events are still held.
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Task finished." },
        });
        yield* fake.emit({ type: "message_end", message: { role: "assistant", content: [] } });
        fake.deferNextState();
        yield* fake.emit({ type: "agent_settled" });
        yield* Queue.take(offers);

        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        // The replayed settle probes Pi, which answers before its prompt
        // preflight is done and so still reports idle.
        yield* fake.takeRequest("get_state");
        yield* fake.resolveDeferredState({
          isStreaming: false,
          isCompacting: false,
          pendingMessageCount: 0,
        });
        // Let that answer reach the event pump ahead of the ack, the order that
        // ended the turn early. The turn must survive the other order too.
        for (let i = 0; i < 100; i++) yield* Effect.yieldNow;
        yield* fake.emit({
          type: "response",
          command: "prompt",
          success: true,
          data: { disposition: "started" },
        });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Hello back." },
        });
        yield* fake.emit({ type: "message_end", message: { role: "assistant", content: [] } });
        yield* fake.emit({ type: "agent_settled" });

        const runId = RunId.make(`run:${THREAD_ID}:1`);
        const replies: Array<string> = [];
        const terminal = yield* takeEvent((event) => {
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.type === "assistant_message" &&
            event.turnItem.runId === runId &&
            event.turnItem.streaming === false
          ) {
            replies.push(event.turnItem.text);
          }
          return event.type === "turn.terminal";
        });
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        // Settling on the stale idle answer would end the turn before its reply.
        assert.include(replies, "Hello back.");
        assert.equal(yield* Queue.size(offers), 0);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("delivers extension notices between turns without starting a run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        method: "notify",
        id: "idle-note",
        message: "[ysk:idle] Heads up · A child finished.",
      });
      const event = yield* takeEvent((e) => e.type === "turn_item.updated");
      assert.isTrue(event.type === "turn_item.updated");
      if (event.type !== "turn_item.updated") return;
      assert.equal(event.turnItem.providerThreadId, providerThread.id);
      assert.isNull(event.turnItem.runId);
      assert.isNull(event.turnItem.providerTurnId);
      assert.equal(event.turnItem.title, "[ysk:idle] Heads up · A child finished.");
      assert.isFalse(fake.allRequests().some((r) => r["type"] === "prompt"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "projects wake job snapshots outside turns and routes stops only to their live scope",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        const snapshot = {
          version: 1,
          scope: "runtime-one",
          id: "1",
          name: "Build",
          command: "make",
          cwd: "/tmp",
          state: "running",
          startedAt: 1000,
          endedAt: null,
          exitCode: null,
          signal: null,
          output: "",
          sourceThreadId: "forged",
          providerSessionId: "forged",
        };
        const send = (data: unknown) =>
          fake.emit({
            type: "extension_ui_request",
            method: "setStatus",
            statusKey: "pi-wake:job",
            statusText: encodeJsonLine(data),
          });
        const takeJob = takeEvent(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "system_notice" &&
            event.turnItem.job !== undefined,
        ).pipe(
          Effect.map((event) => {
            if (
              event.type !== "turn_item.updated" ||
              event.turnItem.type !== "system_notice" ||
              !event.turnItem.job
            )
              throw new Error("Expected job");
            return event.turnItem;
          }),
        );
        yield* send(snapshot);
        const started = yield* takeJob;
        assert.equal(started.job?.providerSessionId, SESSION_ID);
        assert.isUndefined(started.job?.sourceThreadId);
        assert.isNull(started.runId);
        yield* send({ ...snapshot, output: "building" });
        const updated = yield* takeJob;
        assert.equal(updated.id, started.id);
        assert.equal(updated.job?.output, "building");
        const wrong = yield* Effect.exit(
          runtime.stopJob!({ providerThread, scope: "another-runtime", jobId: "1" }),
        );
        assert.equal(wrong._tag, "Failure");
        assert.isFalse(
          fake
            .allRequests()
            .some(
              (request) =>
                request["type"] === "prompt" && request["message"] !== "/t3-background-work",
            ),
        );
        fake.queueCommands({ commands: [{ name: "wake-stop", source: "extension" }] });
        yield* runtime.stopJob!({ providerThread, scope: "runtime-one", jobId: "1" });
        assert.deepEqual(
          fake
            .allRequests()
            .filter(
              (request) =>
                request["type"] === "prompt" && request["message"] !== "/t3-background-work",
            )
            .map((request) => request["message"]),
          ["/wake-stop runtime-one 1"],
        );
        for (const [index, state] of ["succeeded", "failed", "timed_out", "stopped"].entries()) {
          const data = {
            ...snapshot,
            scope: "finished-" + index,
            state,
            output: "last output",
            endedAt: 2500,
            exitCode: state === "succeeded" ? 0 : null,
          };
          yield* fake.emit({
            type: "message_end",
            message: { role: "custom", customType: "pi-wake:job", details: data },
          });
          const ended = yield* takeJob;
          assert.equal(ended.job?.state, state);
          assert.equal(ended.job?.output, "last output");
          assert.equal(DateTime.toEpochMillis(ended.completedAt!), 2500);
          assert.notEqual(ended.id, started.id);
        }
        // Invalid payloads must not leak into the durable stream; a later notification is a fence.
        yield* send({ ...snapshot, output: "x".repeat(16001) });
        yield* fake.emit({ type: "extension_ui_request", method: "notify", message: "fence" });
        const next = yield* takeEvent((event) => event.type === "turn_item.updated");
        assert.isTrue(next.type === "turn_item.updated" && next.turnItem.title === "fence");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "keeps delegated completion pending through background work until the final follow-up settles",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        let providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        // The same keepalive covers a running wake-capable job and its held wake.
        for (const [index, pending] of [true, true, false].entries()) {
          fake.setBackgroundWork(pending);
          yield* startTurn(runtime, providerThread, "default", [], "Work", undefined, index + 1);
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "agent_settled" });
          while (true) {
            const event = yield* takeEvent(() => true);
            if (event.type === "provider_thread.updated") providerThread = event.providerThread;
            if (event.type === "turn.terminal") break;
          }
          assert.equal((providerThread.pendingBackgroundTasks?.length ?? 0) > 0, pending);
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "retains service sessions without blocking completion after readiness and wake consumption",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        let providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        // Starting, held readiness wake, consumed readiness, later exit wake, consumed exit.
        for (const [index, [pending, retained]] of [
          [true, true],
          [true, true],
          [false, true],
          [true, false],
          [false, false],
        ].entries()) {
          fake.setBackgroundWork(pending!, retained);
          yield* startTurn(runtime, providerThread, "default", [], "Work", undefined, index + 1);
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "agent_settled" });
          while (true) {
            const event = yield* takeEvent(() => true);
            if (event.type === "provider_thread.updated") providerThread = event.providerThread;
            if (event.type === "turn.terminal") break;
          }
          assert.equal((providerThread.pendingBackgroundTasks?.length ?? 0) > 0, pending);
          assert.equal(yield* runtime.hasPendingBackgroundWork!, pending);
          assert.equal(yield* runtime.hasRetainedBackgroundServices!, retained);
        }
        // Pi's own queue is authoritative for messages already handed to it.
        fake.setBackgroundWork(false, true);
        fake.queueState({ pendingMessageCount: 1 });
        assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("pins idle sessions despite an MCP warning and releases when extension work ends", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      // This is the injected extension's session_start connection warning,
      // not an extension-load failure: its command remains registered.
      yield* fake.emit({
        type: "extension_ui_request",
        method: "notify",
        message: "t3-code MCP unavailable: connection refused",
        notifyType: "warning",
      });
      for (const pending of [true, false]) {
        fake.setBackgroundWork(pending);
        const probe = yield* Effect.forkChild(runtime.hasPendingBackgroundWork!);
        const request = yield* fake.takeRequest("prompt");
        assert.equal(request["message"], "/t3-background-work");
        yield* fake.emit({
          type: "extension_ui_request",
          method: "setStatus",
          statusKey: "t3:background-work",
          statusText: pending ? "pending" : "idle",
        });
        assert.equal(yield* Fiber.join(probe), pending);
        assert.isFalse(yield* runtime.hasRetainedBackgroundServices!);
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("gives notify and extension-error items distinct ids in each thread", () =>
    Effect.gen(function* () {
      const itemIds: Array<string> = [];
      for (const [threadId, sessionId] of [
        [THREAD_ID, SESSION_ID],
        [ThreadId.make("thread-pi-test-2"), ProviderSessionId.make("provider-session-pi-test-2")],
      ] as const) {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake, "default", threadId, sessionId);
        const providerThread = yield* runtime.ensureThread({
          threadId,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        // Both turns start at the same item ordinal.
        yield* startTurn(
          runtime,
          providerThread,
          "default",
          [],
          "Hello pi",
          undefined,
          1,
          threadId,
        );
        yield* fake.takeRequest("prompt");
        yield* fake.emit({
          type: "extension_ui_request",
          id: "ui-notify",
          method: "notify",
          message: `note from ${threadId}`,
          notifyType: "info",
        });
        yield* fake.emit({
          type: "extension_error",
          extensionPath: "/ext/example.ts",
          event: "tool_call",
          error: "boom",
        });
        for (const prefix of ["notify:", "extension-error:"]) {
          const item = yield* takeEvent(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.nativeItemRef?.nativeId?.startsWith(prefix) === true,
          );
          assert.isTrue(item.type === "turn_item.updated" && item.turnItem.threadId === threadId);
          if (item.type === "turn_item.updated") itemIds.push(item.turnItem.id);
        }
      }
      assert.equal(new Set(itemIds).size, 4);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("injects the T3 MCP extension and bearer when a session exists", () =>
    Effect.gen(function* () {
      McpProviderSession.setMcpProviderSession({
        environmentId: EnvironmentId.make("environment-pi-mcp"),
        threadId: THREAD_ID,
        providerSessionId: "mcp-session-pi",
        providerInstanceId: PI_INSTANCE_ID,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer secret-pi-token",
        browserToolsAvailable: true,
      });
      const fake = yield* makeFakePi;
      yield* openRuntime(fake);
      const spawn = fake.lastSpawn();
      assert.isTrue(spawn.args.includes("--extension"));
      const extensions = spawn.args.flatMap((arg, index) =>
        arg === "--extension" ? [spawn.args[index + 1]] : [],
      );
      assert.isFalse(spawn.args.includes("--no-extensions"));
      assert.isTrue(extensions.some((path) => path?.endsWith("pi-t3-mcp-extension.ts")));
      assert.equal(spawn.env.T3_MCP_URL, "http://127.0.0.1:43123/mcp");
      assert.equal(spawn.env.T3_MCP_BEARER_TOKEN, "secret-pi-token");
      assert.equal(spawn.env.T3_PI_RUNTIME_MODE, "full-access");
    }).pipe(
      Effect.ensuring(Effect.sync(() => McpProviderSession.clearMcpProviderSession(THREAD_ID))),
      Effect.scoped,
      Effect.provide(layerTest),
    ),
  );

  it.effect("rejects a resume while a turn is active", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const error = yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      assert.equal(error._tag, "ProviderAdapterResumeThreadError");
      assert.match(String(error.cause), /while a turn is active/);
      const rollbackError = yield* runtime
        .rollbackThread(rollbackInput(providerThread))
        .pipe(Effect.flip);
      assert.equal(rollbackError._tag, "ProviderAdapterRollbackThreadError");
      assert.match(String(rollbackError.cause), /while a Pi turn is active/);
      assert.isFalse(fake.allRequests().some((request) => request.type === "fork"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("waits for a slow Pi resume without starting a replacement", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextRequest("switch_session");
      const resumed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.forkChild);
      const request = yield* fake.takeRequest("switch_session");
      yield* TestClock.adjust(Duration.millis(16_820));
      yield* fake.emit({
        type: "response",
        id: request.id,
        command: "switch_session",
        success: true,
        data: { cancelled: false },
      });
      assert.equal((yield* Fiber.join(resumed)).nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
      assert.isFalse(fake.allRequests().some((request) => request.type === "new_session"));
      yield* startTurn(runtime, providerThread, "default");
      yield* fake.takeRequest("prompt");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("creates a distinct native session after a failed resume", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.vetoNextSwitch();
      yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      const replacement = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.equal(replacement.id, providerThread.id);
      assert.notEqual(
        replacement.nativeThreadRef?.nativeId,
        providerThread.nativeThreadRef?.nativeId,
      );
      assert.equal(
        fake.allRequests().filter((request) => request.type === "new_session").length,
        1,
      );
      yield* startTurn(runtime, replacement, "default");
      yield* fake.takeRequest("prompt");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["veto", "same identity"] as const)(
    "rejects a replacement with %s",
    (invalidReplacement) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        if (invalidReplacement === "veto") fake.vetoNextNewSession();
        else fake.queueState({ sessionFile: FAKE_SESSION_FILE });
        const error = yield* runtime
          .ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
            existingProviderThread: { ...providerThread, nativeThreadRef: null },
          })
          .pipe(Effect.flip);
        assert.equal(error._tag, "ProviderAdapterEnsureThreadError");
        assert.match(
          String(error.cause),
          invalidReplacement === "veto" ? /cancelled/ : /distinct session/,
        );
        yield* startTurn(runtime, providerThread, "default").pipe(Effect.flip);
        assert.isFalse(fake.allRequests().some((request) => request.type === "prompt"));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("retires a timed-out lifecycle process before a late switch can race replacement", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextRequest("switch_session");
      const resumed = yield* runtime
        .resumeThread({ providerThread })
        .pipe(Effect.flip, Effect.forkChild);
      const request = yield* fake.takeRequest("switch_session");
      yield* TestClock.adjust(Duration.seconds(60));
      const error = yield* Fiber.join(resumed);
      assert.match(String(error.cause), /timed out after 60000ms/);
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "error",
      );
      yield* fake.emit({
        type: "response",
        id: request.id,
        command: "switch_session",
        success: true,
        data: { cancelled: false },
      });
      yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
          existingProviderThread: { ...providerThread, nativeThreadRef: null },
        })
        .pipe(Effect.flip);
      assert.isFalse(
        fake
          .allRequests()
          .some((request) => request.type === "new_session" || request.type === "prompt"),
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "uses the lifecycle deadline for fresh sessions and drops replaced native metadata",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.deferNextRequest("new_session");
        fake.queueState({ sessionFile: "/fake/fresh-after-delay.jsonl" });
        const replacing = yield* runtime
          .ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
            existingProviderThread: {
              ...providerThread,
              nativeThreadRef: null,
              nativeConversationHeadRef: {
                driver: PI_PROVIDER,
                nativeId: "old-leaf",
                strength: "strong",
              },
              contextUsage: { usedTokens: 314_551, maxTokens: 1_000_000 },
            },
          })
          .pipe(Effect.forkChild);
        const request = yield* fake.takeRequest("new_session");
        yield* TestClock.adjust(Duration.millis(16_820));
        yield* fake.emit({
          type: "response",
          id: request.id,
          command: "new_session",
          success: true,
          data: { cancelled: false },
        });
        const replacement = yield* Fiber.join(replacing);
        assert.equal(replacement.nativeThreadRef?.nativeId, "/fake/fresh-after-delay.jsonl");
        assert.isNull(replacement.contextUsage);
        assert.isNull(replacement.nativeConversationHeadRef);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("replaces a native session when the first resume's state refresh fails", () =>
    Effect.gen(function* () {
      const original = yield* makeFakePi;
      const originalRuntime = yield* openRuntime(original);
      const providerThread = yield* originalRuntime.runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      fake.failNextState();
      yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      const replacement = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.notEqual(
        replacement.nativeThreadRef?.nativeId,
        providerThread.nativeThreadRef?.nativeId,
      );
      yield* startTurn(runtime, replacement, "default");
      yield* fake.takeRequest("prompt");
      assert.isTrue(fake.allRequests().some((request) => request.type === "new_session"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("retires an interrupted switch before accepting further requests", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextRequest("switch_session");
      const resumed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.forkChild);
      yield* fake.takeRequest("switch_session");
      yield* Fiber.interrupt(resumed);
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "error",
      );
      yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
          existingProviderThread: { ...providerThread, nativeThreadRef: null },
        })
        .pipe(Effect.flip);
      assert.isFalse(fake.allRequests().some((request) => request.type === "new_session"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("budgets legacy native history with Pi's selected model capacity", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueState({
        sessionFile: FAKE_SESSION_FILE,
        model: { provider: "anthropic", id: "large", contextWindow: 1_000_000 },
      });
      fake.queueModels([{ provider: "anthropic", id: "small", contextWindow: 32_000 }]);
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const budget = (model: string) =>
        handoffBudget({
          tokenCap: 16_000,
          userText: "$handoff",
          attachments: [],
          providerThread,
          nativeContextEstimate: 307_543,
          modelContextWindow: runtime.getModelContextWindow?.(modelSelection(model)),
        });
      assert.equal(budget("default"), 16_000);
      assert.equal(budget("anthropic/large"), 16_000);
      assert.equal(runtime.getModelContextWindow?.(modelSelection("anthropic/small")), 32_000);
      assert.equal(budget("anthropic/small"), 0);
      assert.isUndefined(runtime.getModelContextWindow?.(modelSelection("anthropic/unknown")));
      assert.isUndefined(
        runtime.getModelContextWindow?.({
          instanceId: ProviderInstanceId.make("other-pi"),
          model: "anthropic/large",
        }),
      );
      // New native sessions have their own default, even within one process.
      fake.queueState({
        sessionFile: "/fake/replacement.jsonl",
        model: { provider: "anthropic", id: "small", contextWindow: 32_000 },
      });
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.equal(runtime.getModelContextWindow?.(modelSelection("default")), 32_000);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["completed", "interrupted", "transport-death"] as const)(
    "adopts the run's provider thread identity and scopes %s snapshots to its attempt",
    (settlement) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const now = yield* DateTime.now;
        // The placeholder row the orchestrator creates for a first run: no
        // native identity yet. The adapter must bind the pi session to this
        // row instead of registering a second session-file-keyed row, or the
        // projection ends up with two live rows per app thread.
        const placeholder: OrchestrationV2ProviderThread = {
          id: ProviderThreadId.make(
            "thread:provider:pi:native-thread:pending:run:thread-pi-test:1",
          ),
          driver: PI_PROVIDER,
          providerInstanceId: PI_INSTANCE_ID,
          providerSessionId: SESSION_ID,
          appThreadId: THREAD_ID,
          ownerNodeId: null,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
          status: "not_loaded",
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          pendingBackgroundTasks: [{ taskId: "pi:background-work", kind: "background_task" }],
          createdAt: now,
          updatedAt: now,
        };
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
          existingProviderThread: placeholder,
        });
        assert.equal(providerThread.id, placeholder.id);
        assert.equal(providerThread.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
        assert.isFalse(fake.allRequests().some((request) => request.type === "new_session"));
        const updated = yield* takeEvent((event) => event.type === "provider_thread.updated");
        assert.isTrue(
          updated.type === "provider_thread.updated" &&
            updated.providerThread.id === placeholder.id,
        );
        if (updated.type !== "provider_thread.updated") return;
        assert.isUndefined(
          updated.runAttemptId,
          "loading is a lifecycle update, not a turn update",
        );
        yield* startTurn(runtime, providerThread);
        const running = yield* takeEvent(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running",
        );
        if (running.type !== "provider_turn.updated")
          return yield* Effect.die("Expected running turn");
        const active = yield* takeEvent(
          (event) =>
            event.type === "provider_thread.updated" && event.providerThread.status === "active",
        );
        if (active.type !== "provider_thread.updated")
          return yield* Effect.die("Expected active thread");
        const attemptId = RunAttemptId.make(`run-attempt:run:${THREAD_ID}:1:1`);
        assert.equal(active.runAttemptId, attemptId);
        yield* fake.emit({ type: "agent_start" });
        if (settlement === "transport-death") {
          yield* fake.closeStdout;
        } else {
          if (settlement === "interrupted") {
            yield* runtime.interruptTurn({
              providerThread,
              providerTurnId: running.providerTurn.id,
            });
          }
          yield* fake.emit({ type: "agent_settled" });
        }
        const snapshots: Array<
          Extract<ProviderAdapterV2Event, { type: "provider_thread.updated" }>
        > = [];
        const terminal = yield* takeEvent((event) => {
          if (event.type === "provider_thread.updated") snapshots.push(event);
          return event.type === "turn.terminal";
        });
        if (terminal.type !== "turn.terminal") return yield* Effect.die("Expected terminal");
        assert.equal(terminal.status, settlement === "transport-death" ? "failed" : settlement);
        assert.isAtLeast(snapshots.length, 2, "roster refresh and final idle snapshot");
        for (const snapshot of snapshots) {
          assert.equal(
            snapshot.runAttemptId,
            attemptId,
            "roster and finalization must retain the origin after clearing activeTurn",
          );
          assert.equal(snapshot.providerThread.id, placeholder.id);
        }
        assert.deepEqual(snapshots[0]?.providerThread.pendingBackgroundTasks, []);
        assert.equal(snapshots.at(-1)?.providerThread.status, "idle");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("resets applied thinking when returning to Pi default", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueState({
        model: { provider: "xai", id: "grok-4.6" },
        thinkingLevel: "medium",
        sessionFile: FAKE_SESSION_FILE,
        sessionId: "abc",
      });
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      // An explicit effort on a concrete model.
      yield* startTurn(runtime, providerThread, "default", [], "Hello pi", {
        instanceId: PI_INSTANCE_ID,
        model: "xai/grok-4.6",
        options: [{ id: "thinking", value: "high" }],
      });
      const modelRequest = yield* fake.takeRequest("set_model");
      assert.equal(modelRequest["provider"], "xai");
      assert.equal(modelRequest["modelId"], "grok-4.6");
      const levelRequest = yield* fake.takeRequest("set_thinking_level");
      assert.equal(levelRequest["level"], "high");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [], willRetry: false });
      yield* fake.emit({ type: "agent_settled" });
      yield* takeEvent((event) => event.type === "turn.terminal");

      // Back to Pi default with no explicit thinking choice of its own.
      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Hello pi",
        {
          instanceId: PI_INSTANCE_ID,
          model: "default",
        },
        2,
      );
      const replayModel = yield* fake.takeRequest("set_model");
      assert.equal(replayModel["provider"], "xai");
      assert.equal(replayModel["modelId"], "grok-4.6");
      const resetLevel = yield* fake.takeRequest("set_thinking_level");
      assert.equal(resetLevel["level"], "medium");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("expands a selected $ skill through Pi's native skill command", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueCommands({
        commands: [
          {
            name: "skill:repo-review",
            description: "Review this repository.",
            source: "skill",
            sourceInfo: {
              path: "/workspace/.agents/skills/repo-review/SKILL.md",
              scope: "project",
            },
          },
        ],
      });
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Review this change please $repo-review",
      );
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "/skill:repo-review Review this change please");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("expands every selected $ skill through Pi native skill commands", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueCommands({
        commands: [
          {
            name: "skill:repo-review",
            source: "skill",
            sourceInfo: {
              path: "/workspace/.agents/skills/repo-review/SKILL.md",
              scope: "project",
            },
          },
          {
            name: "skill:deploy",
            source: "skill",
            sourceInfo: {
              path: "/workspace/.agents/skills/deploy/SKILL.md",
              scope: "project",
            },
          },
        ],
      });
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* startTurn(runtime, providerThread, "default", [], "use $repo-review and $deploy");
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "/skill:repo-review /skill:deploy use  and");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { historical: false, label: "the latest turn" },
    { historical: true, label: "a historical turn" },
  ])("natively forks $label into an independent session", ({ historical }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const forkFake = yield* makeFakePi;
      const forkFile = "/fake/forked.jsonl";
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        forkFake,
      );
      const source = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const turn = (ordinal: number): OrchestrationV2ProviderTurn => ({
        id: ProviderTurnId.make(`turn-${ordinal}`),
        providerThreadId: source.id,
        nodeId: NodeId.make(`node-${ordinal}`),
        runAttemptId: null,
        nativeTurnRef: { driver: PI_PROVIDER, nativeId: `u${ordinal}`, strength: "strong" },
        ordinal,
        status: "completed",
        startedAt: null,
        completedAt: null,
      });
      forkFake.queueState({ sessionFile: forkFile });
      fake.queueState({ sessionFile: forkFile });
      const target = ThreadId.make("fork-target");
      const forked = yield* runtime.forkThread({
        sourceProviderThread: source,
        sourceProviderTurns: historical ? [turn(1), turn(2)] : [turn(1)],
        providerTurnId: turn(1).id,
        targetThreadId: target,
      });
      assert.equal(forked.appThreadId, target);
      assert.equal(forked.nativeThreadRef?.nativeId, forkFile);
      assert.notEqual(forked.id, source.id);
      assert.equal(source.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
      const args = forkFake.lastSpawn().args;
      assert.equal(args[args.indexOf("--fork") + 1], FAKE_SESSION_FILE);
      assert.include(args, "--no-extensions");
      assert.include(args, "--no-tools");
      assert.notInclude(args, "--no-session");
      assert.deepEqual(
        forkFake
          .allRequests()
          .filter((request) => request.type === "fork")
          .map((request) => request.entryId),
        historical ? ["u2"] : [],
      );
      assert.isFalse(
        fake.allRequests().some((request) => request.type === "fork" || request.type === "clone"),
      );
      // ProviderTurnStartService adopts the fork into its pending row.
      const adopted = { ...forked, id: ProviderThreadId.make("pending-fork-row") };
      yield* startTurn(runtime, adopted, "default", [], "Continue", undefined, 1, target);
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_settled" });
      const updated = yield* takeEvent(
        (event) =>
          event.type === "provider_thread.updated" && event.providerThread.appThreadId === target,
      );
      assert.isTrue(
        updated.type === "provider_thread.updated" && updated.providerThread.id === adopted.id,
      );
      yield* takeEvent((event) => event.type === "turn.terminal");
      yield* runtime.resumeThread({ providerThread: adopted });
      assert.equal(
        fake.allRequests().findLast((request) => request.type === "switch_session")?.sessionPath,
        forkFile,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("presents explicitly namespaced MCP extension tools", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      for (const type of ["tool_execution_start", "tool_execution_end"]) {
        yield* fake.emit({
          type,
          toolCallId: "weather-call",
          toolName: "mcp__weather__get_weather",
          args: { city: "Berlin" },
          result: { content: [{ type: "text", text: "Sunny" }] },
          isError: false,
        });
        const event = yield* takeEvent(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
        );
        if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
          return yield* Effect.die("Expected an MCP tool item");
        assert.equal(event.turnItem.title, "get weather");
        assert.equal(
          event.turnItem.status,
          type === "tool_execution_start" ? "running" : "completed",
        );
        assert.deepEqual(event.turnItem.toolSource, {
          key: "mcp:weather",
          name: "weather",
          kind: "integration",
        });
        assert.deepEqual(event.turnItem.input, { city: "Berlin" });
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("observes official subagent results without inventing child threads", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "tool_execution_update",
        toolCallId: "call_sub",
        toolName: "subagent",
        partialResult: {
          content: [{ type: "text", text: "(running...)" }],
          details: {
            mode: "single",
            results: [
              {
                agent: "scout",
                task: "map the repo",
                exitCode: 0,
                stderr: "",
                sessionFile: "/ignored/custom-extension-session.jsonl",
                messages: [
                  { role: "assistant", content: [{ type: "text", text: "scanning files" }] },
                ],
              },
            ],
          },
        },
      });
      const running = yield* takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "running",
      );
      assert.isTrue(
        running.type === "subagent.updated" &&
          running.subagent.title === "scout" &&
          running.subagent.prompt === "map the repo" &&
          running.subagent.progress === "scanning files" &&
          running.subagent.childThreadId === null,
      );

      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_sub",
        toolName: "subagent",
        isError: false,
        result: {
          content: [{ type: "text", text: "done" }],
          details: {
            mode: "single",
            results: [
              {
                agent: "scout",
                task: "map the repo",
                exitCode: 0,
                stopReason: "stop",
                stderr: "",
                messages: [
                  { role: "assistant", content: [{ type: "text", text: "repo has one file" }] },
                ],
              },
            ],
          },
        },
      });
      const doneCard = yield* takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "completed",
      );
      assert.isTrue(
        doneCard.type === "subagent.updated" &&
          doneCard.subagent.result === "repo has one file" &&
          doneCard.subagent.childThreadId === null,
      );
      const subagentItem = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "subagent" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        subagentItem.type === "turn_item.updated" &&
          subagentItem.turnItem.type === "subagent" &&
          subagentItem.turnItem.childThreadId === null,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("persists edit patches and write content on file change items", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const patch = "--- a.ts\n+++ a.ts\n@@ -1 +1 @@\n-old\n+new\n";
      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call_edit",
        toolName: "edit",
        args: { path: "a.ts", edits: [{ oldText: "old", newText: "new" }] },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_edit",
        toolName: "edit",
        isError: false,
        result: {
          content: [{ type: "text", text: "Successfully replaced 1 block(s) in a.ts." }],
          details: { diff: "-1 old\n+1 new", patch, firstChangedLine: 1 },
        },
      });
      const edit = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "file_change" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        edit.type === "turn_item.updated" &&
          edit.turnItem.type === "file_change" &&
          edit.turnItem.fileName === "a.ts" &&
          edit.turnItem.diffStr === patch,
      );

      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call_write",
        toolName: "write",
        args: { path: "b.ts", content: "export {};\n" },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_write",
        toolName: "write",
        isError: false,
        result: { content: [{ type: "text", text: "Successfully wrote to b.ts" }] },
      });
      const write = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "file_change" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        write.type === "turn_item.updated" &&
          write.turnItem.type === "file_change" &&
          write.turnItem.fileName === "b.ts" &&
          write.turnItem.newStr === "export {};\n",
      );

      // A failed edit has no patch, so it keeps the error to show when expanded.
      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call_edit_failed",
        toolName: "edit",
        args: { path: "c.ts", edits: [{ oldText: "missing", newText: "new" }] },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_edit_failed",
        toolName: "edit",
        isError: true,
        result: { content: [{ type: "text", text: "Could not find the text in c.ts." }] },
      });
      const failedEdit = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "file_change" &&
          event.turnItem.status === "failed",
      );
      assert.isTrue(
        failedEdit.type === "turn_item.updated" &&
          failedEdit.turnItem.type === "file_change" &&
          failedEdit.turnItem.diffStr === "Could not find the text in c.ts.",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("settles a command-only prompt from its deferred ack and idle probe", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/command-only");
      yield* fake.takeRequest("prompt");
      // A pure extension command: dialog + notify, then the deferred ack —
      // pi emits no agent_start/agent_settled at all.
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-cmd",
        method: "notify",
        message: "/op:status done\n\nTask: alpha",
        notifyType: "info",
      });
      const notice = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "system_notice",
      );
      assert.isTrue(
        notice.type === "turn_item.updated" &&
          notice.turnItem.type === "system_notice" &&
          notice.turnItem.message === "/op:status done\n\nTask: alpha",
      );
      yield* fake.emit({ type: "response", command: "prompt", success: true });
      // The adapter probes get_state (auto-acked idle by the fake), then
      // settles the turn as completed.
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("leaves /compacted as an ordinary prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compacted please");
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "/compacted please");
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "compact"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails a compact that never started", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compact");
      yield* fake.takeRequest("compact");
      yield* fake.emit({
        type: "response",
        command: "compact",
        success: false,
        error: "Nothing to compact (session too small)",
      });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(
        terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message === "Nothing to compact (session too small)",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("restarts Pi when Stop interrupts a user compact", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compact");
      yield* fake.takeRequest("compact");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "abort"));
      yield* fake.closeStdout;
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("steers /compact as RPC compact instead of a prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      yield* fake.emit({ type: "agent_start" });
      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run:thread-pi-test:1"),
        providerThread,
        providerTurnId: providerTurnId!,
        message: {
          messageId: "message:thread-pi-test:steer-compact" as never,
          text: "/compact keep the tests",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      const compact = yield* fake.takeRequest("compact");
      assert.equal(compact["customInstructions"], "keep the tests");
      assert.isUndefined(compact["streamingBehavior"]);
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      fake.queueState({ isStreaming: false, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
        aborted: false,
        willRetry: false,
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.emit({ type: "response", command: "compact", success: true });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("persists current xAI capacity text for the thread error banner", () =>
    expectModelFailure("The model is currently at capacity due to high demand."),
  );

  it.effect("persists extension-normalized xAI capacity text for the thread error banner", () =>
    expectModelFailure(
      "Provider overloaded: The model is currently at capacity due to high demand.",
    ),
  );

  it.effect("emits session-start dialogs before a turn exists", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      // Project-trust style prompt before any turn exists.
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-trust",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .pi/extensions.",
      });
      const pending = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      const requestId =
        pending.type === "runtime_request.updated" ? pending.runtimeRequest.id : undefined;
      yield* runtime.respondToRuntimeRequest({ requestId: requestId!, decision: "accept" });
      const uiResponse = yield* fake.takeRequest("extension_ui_response");
      assert.equal(uiResponse["id"], "ui-trust");
      assert.equal(uiResponse["confirmed"], true);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("remembers session approvals only for identical confirmation content", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      // Project-trust style prompt before any turn exists.
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-trust",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .pi/extensions.",
      });
      const pending = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      const requestId =
        pending.type === "runtime_request.updated" ? pending.runtimeRequest.id : undefined;
      yield* runtime.respondToRuntimeRequest({
        requestId: requestId!,
        decision: "acceptForSession",
      });
      const uiResponse = yield* fake.takeRequest("extension_ui_response");
      assert.equal(uiResponse["id"], "ui-trust");
      assert.equal(uiResponse["confirmed"], true);
      // Field order and request id do not change the confirmation content.
      yield* fake.emit({
        message: "This project has .pi/extensions.",
        title: "Run project extensions?",
        method: "confirm",
        id: "ui-trust-again",
        type: "extension_ui_request",
      });
      assert.equal((yield* fake.takeRequest("extension_ui_response"))["id"], "ui-trust-again");
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-other",
        method: "confirm",
        title: "Run project extensions?",
        message: "A different project.",
      });
      const other = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      assert.isTrue(
        other.type === "runtime_request.updated" &&
          other.runtimeRequest.nativeRequestRef?.nativeId === "ui-other",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    "new thread",
    "switch",
    "rollback",
    "empty rollback",
    "fork",
    "pending switch",
    "pending rollback",
  ] as const)("does not reuse session approvals after %s", (boundary) =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const forkFake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        forkFake,
      );
      const source = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const prompt = {
        type: "extension_ui_request",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .pi/extensions.",
      };
      yield* fake.emit({ ...prompt, id: "grant-in-a" });
      const granted = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      assert.equal(granted.type, "runtime_request.updated");
      if (granted.type !== "runtime_request.updated") return;
      const pendingApproval = boundary === "pending switch" || boundary === "pending rollback";
      if (!pendingApproval) {
        yield* runtime.respondToRuntimeRequest({
          requestId: granted.runtimeRequest.id,
          decision: "acceptForSession",
        });
        assert.equal((yield* fake.takeRequest("extension_ui_response"))["confirmed"], true);
      }
      const targetThreadId = ThreadId.make("thread-pi-b");
      if (boundary === "new thread") {
        yield* runtime.ensureThread({
          threadId: targetThreadId,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
      } else if (boundary === "switch" || boundary === "pending switch") {
        fake.queueState({ sessionFile: "/fake/b.jsonl" });
        yield* runtime.resumeThread({
          providerThread: {
            ...source,
            id: ProviderThreadId.make("provider-thread-pi-b"),
            appThreadId: targetThreadId,
            nativeThreadRef: { driver: PI_PROVIDER, nativeId: "/fake/b.jsonl", strength: "strong" },
          },
        });
      } else if (
        boundary === "rollback" ||
        boundary === "empty rollback" ||
        boundary === "pending rollback"
      ) {
        fake.queueState({ sessionFile: "/fake/rolled-back.jsonl" });
        yield* runtime.rollbackThread({
          providerThread: source,
          target: {
            type: "thread_start",
            checkpointId: CheckpointId.make("checkpoint-pi"),
            appRunOrdinal: 0,
          },
          providerThreadTurns:
            boundary === "empty rollback"
              ? []
              : [
                  {
                    id: ProviderTurnId.make("pi-turn-to-discard"),
                    providerThreadId: source.id,
                    nodeId: NodeId.make("pi-discard-node"),
                    runAttemptId: null,
                    nativeTurnRef: {
                      driver: PI_PROVIDER,
                      nativeId: "user-entry",
                      strength: "strong",
                    },
                    ordinal: 1,
                    status: "completed",
                    startedAt: null,
                    completedAt: null,
                  },
                ],
        });
      } else if (boundary === "fork") {
        forkFake.queueState({ sessionFile: "/fake/fork.jsonl" });
        fake.queueState({ sessionFile: "/fake/fork.jsonl" });
        yield* runtime.forkThread({ sourceProviderThread: source, targetThreadId });
      }
      if (pendingApproval) {
        const cancelled = yield* takeEvent(
          (event) =>
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.id === granted.runtimeRequest.id &&
            event.runtimeRequest.status === "cancelled",
        );
        assert.equal(cancelled.type, "runtime_request.updated");
        assert.equal((yield* fake.takeRequest("extension_ui_response"))["cancelled"], true);
        const lateResponse = yield* Effect.result(
          runtime.respondToRuntimeRequest({
            requestId: granted.runtimeRequest.id,
            decision: "acceptForSession",
          }),
        );
        assert.equal(lateResponse._tag, "Failure");
      }
      yield* fake.emit({
        ...prompt,
        id: "ask-again",
      });
      // An automatic wire response must fail the test instead of waiting
      // indefinitely for the pending request it would suppress.
      const asked = yield* Effect.race(
        takeEvent(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        ),
        fake.takeRequest("extension_ui_response").pipe(Effect.as(null)),
      );
      assert.isNotNull(asked);
      if (asked === null) return;
      assert.equal(asked.type, "runtime_request.updated");
      if (asked.type !== "runtime_request.updated") return;
      assert.equal(asked.runtimeRequest.nativeRequestRef?.nativeId, "ask-again");
      if (
        boundary === "new thread" ||
        boundary === "switch" ||
        boundary === "pending switch" ||
        boundary === "fork"
      ) {
        assert.equal(asked.threadId, targetThreadId);
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["switch", "rollback"] as const)(
    "does not restore session approvals when a send finishes after %s",
    (boundary) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const sending = yield* Deferred.make<void>();
        const releaseSend = yield* Deferred.make<void>();
        const originalMakeConnection = PiRpc.makePiRpcConnection;
        // PiRpc.send returns after enqueueing, so delaying the child stdin sink
        // would not hold the adapter's send. Defer that transport call itself.
        yield* Effect.acquireRelease(
          Effect.sync(() =>
            vi.spyOn(PiRpc, "makePiRpcConnection").mockImplementation((options) =>
              originalMakeConnection(options).pipe(
                Effect.map((connection) => ({
                  ...connection,
                  send: (record) =>
                    Effect.gen(function* () {
                      if (record["id"] === "late-grant" && record["confirmed"] === true) {
                        yield* Deferred.succeed(sending, undefined);
                        yield* Deferred.await(releaseSend);
                      }
                      yield* connection.send(record);
                    }),
                })),
              ),
            ),
          ),
          (spy) => Effect.sync(() => spy.mockRestore()),
        );
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const source = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        const prompt = {
          type: "extension_ui_request",
          method: "confirm",
          title: "Run project extensions?",
          message: "This project has .pi/extensions.",
        };
        yield* fake.emit({ ...prompt, id: "late-grant" });
        const granted = yield* takeEvent(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        );
        assert.equal(granted.type, "runtime_request.updated");
        if (granted.type !== "runtime_request.updated") return;
        // Call the adapter runtime directly, before any boundary cancels the request.
        const response = yield* runtime
          .respondToRuntimeRequest({
            requestId: granted.runtimeRequest.id,
            decision: "acceptForSession",
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(sending);
        // The newer adapter drains session events under the same permit as
        // response publication. Hold the boundary in a fiber until the send
        // is released, while observing cancellation before that release.
        const replacement = yield* (
          boundary === "switch"
            ? runtime.ensureThread({
                threadId: ThreadId.make("thread-pi-after-late-grant"),
                modelSelection: modelSelection("default"),
                runtimePolicy,
              })
            : runtime.rollbackThread({
                providerThread: source,
                target: {
                  type: "thread_start",
                  checkpointId: CheckpointId.make("checkpoint-pi-late-grant"),
                  appRunOrdinal: 0,
                },
                providerThreadTurns: [],
              })
        ).pipe(Effect.forkScoped);
        yield* takeEvent(
          (event) =>
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.id === granted.runtimeRequest.id &&
            event.runtimeRequest.status === "cancelled",
        );
        const cancellation = yield* fake.takeRequest("extension_ui_response");
        assert.equal(cancellation["id"], "late-grant");
        assert.equal(cancellation["cancelled"], true);
        yield* Deferred.succeed(releaseSend, undefined);
        yield* Fiber.join(response);
        yield* Fiber.join(replacement);
        const lateResponse = yield* fake.takeRequest("extension_ui_response");
        assert.equal(lateResponse["id"], "late-grant");
        assert.equal(lateResponse["confirmed"], true);

        yield* fake.emit({ ...prompt, id: "after-late-grant" });
        const next = yield* Effect.race(
          takeEvent((event) => event.type === "runtime_request.updated"),
          fake.takeRequest("extension_ui_response").pipe(Effect.as(null)),
        );
        assert.isNotNull(next);
        if (next === null || next.type !== "runtime_request.updated") return;
        // A late response must neither resolve the cancelled request nor cache its approval.
        assert.equal(next.runtimeRequest.status, "pending");
        assert.equal(next.runtimeRequest.nativeRequestRef?.nativeId, "after-late-grant");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["changed", "unreadable"] as const)(
    "rechecks cached approvals before settlement when native session is %s",
    (identity) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        const prompt = {
          type: "extension_ui_request",
          method: "confirm",
          title: "Run project extensions?",
          message: "This project has .pi/extensions.",
        };
        yield* fake.emit({ ...prompt, id: "original-grant" });
        const granted = yield* takeEvent(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        );
        if (granted.type !== "runtime_request.updated") return;
        yield* runtime.respondToRuntimeRequest({
          requestId: granted.runtimeRequest.id,
          decision: "acceptForSession",
        });
        yield* fake.takeRequest("extension_ui_response");
        yield* startTurn(runtime, providerThread, "default", [], "/extension-command");
        yield* fake.takeRequest("prompt");
        // No terminal event: a same-session confirmation still reuses the grant.
        yield* fake.emit({ ...prompt, id: "unchanged-before-terminal" });
        const cached = yield* fake.takeRequest("extension_ui_response");
        assert.equal(cached["id"], "unchanged-before-terminal");
        assert.equal(cached["confirmed"], true);
        // Pi's slash command switches internally before asking the same question.
        if (identity === "changed") fake.queueState({ sessionFile: "/fake/internal-switch.jsonl" });
        else fake.failNextState();
        yield* fake.emit({ ...prompt, id: "foreign-before-terminal" });
        const askedAgain = yield* Effect.race(
          takeEvent(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
          ).pipe(Effect.as(true)),
          fake.takeRequest("extension_ui_response").pipe(Effect.as(false)),
        );
        assert.isTrue(askedAgain);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["settle probe", "failed settle probe"] as const)(
    "drops session approvals when pi moved to another session mid-turn via %s",
    (path) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        const prompt = {
          type: "extension_ui_request",
          method: "confirm",
          title: "Run project extensions?",
          message: "This project has .pi/extensions.",
        };
        yield* fake.emit({ ...prompt, id: "grant-in-turn" });
        const granted = yield* takeEvent(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        );
        assert.equal(granted.type, "runtime_request.updated");
        if (granted.type !== "runtime_request.updated") return;
        yield* runtime.respondToRuntimeRequest({
          requestId: granted.runtimeRequest.id,
          decision: "acceptForSession",
        });
        assert.equal((yield* fake.takeRequest("extension_ui_response"))["confirmed"], true);

        const runTurn = (ordinal: number) =>
          Effect.gen(function* () {
            yield* startTurn(
              runtime,
              providerThread,
              "default",
              [],
              path === "settle probe" ? "Hello pi" : "/extension-command",
              undefined,
              ordinal,
            );
            yield* fake.takeRequest("prompt");
            if (path === "settle probe") {
              yield* fake.emit({ type: "agent_start" });
              yield* fake.emit({ type: "agent_settled" });
            } else {
              fake.failNextState();
              yield* fake.emit({ type: "response", command: "prompt", success: true });
            }
            yield* takeEvent((event) => event.type === "turn.terminal");
          });

        // A successful probe supplies the identity without another state request.
        const stateRequestCount = () =>
          fake.allRequests().filter((request) => request["type"] === "get_state").length;
        const before = stateRequestCount();
        yield* runTurn(1);
        assert.equal(stateRequestCount() - before, path === "settle probe" ? 1 : 2);
        // Same session: the cached approval still applies on the next turn.
        yield* fake.emit({ ...prompt, id: "same-session" });
        assert.equal((yield* fake.takeRequest("extension_ui_response"))["id"], "same-session");

        // A slash command in pi switched sessions with no adapter command.
        // A failed probe leaves the queued identity for the fallback lookup.
        fake.queueState({ sessionFile: "/fake/some-other-session.jsonl" });
        yield* runTurn(2);
        yield* fake.emit({ ...prompt, id: "other-session" });
        // A cached approval answers pi directly and never raises a request, so
        // racing the two outcomes names the regression instead of timing out.
        const askedAgain = yield* Effect.race(
          takeEvent(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
          ).pipe(Effect.as(true)),
          fake.takeRequest("extension_ui_response").pipe(Effect.as(false)),
        );
        assert.isTrue(askedAgain);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("drops session approvals when both command-only state reads fail", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const prompt = {
        type: "extension_ui_request",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .pi/extensions.",
      };
      yield* fake.emit({ ...prompt, id: "grant-before-unreadable-state" });
      const granted = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      assert.equal(granted.type, "runtime_request.updated");
      if (granted.type !== "runtime_request.updated") return;
      yield* runtime.respondToRuntimeRequest({
        requestId: granted.runtimeRequest.id,
        decision: "acceptForSession",
      });
      assert.equal((yield* fake.takeRequest("extension_ui_response"))["confirmed"], true);

      const runCommand = Effect.fnUntraced(function* (ordinal: number, failedReads = 0) {
        yield* startTurn(
          runtime,
          providerThread,
          "default",
          [],
          "/extension-command",
          undefined,
          ordinal,
        );
        yield* fake.takeRequest("prompt");
        if (failedReads > 0) fake.failNextState(failedReads);
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      });

      // A readable, unchanged identity keeps the cached approval after a command-only turn.
      yield* runCommand(1);
      assert.equal(fake.failedStateReads(), 0);
      yield* fake.emit({ ...prompt, id: "same-readable-session" });
      const cached = yield* fake.takeRequest("extension_ui_response");
      assert.equal(cached["id"], "same-readable-session");
      assert.equal(cached["confirmed"], true);

      const before = fake.allRequests().filter((request) => request["type"] === "get_state").length;
      // No foreign session is supplied: both the settle probe and finalize lookup fail.
      yield* runCommand(2, 2);
      assert.equal(
        fake.allRequests().filter((request) => request["type"] === "get_state").length - before,
        2,
      );
      assert.equal(fake.failedStateReads(), 2);

      yield* fake.emit({ ...prompt, id: "ask-after-unreadable-state" });
      const asked = yield* Effect.race(
        takeEvent(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        ),
        fake.takeRequest("extension_ui_response").pipe(Effect.as(null)),
      );
      assert.isNotNull(asked);
      if (asked === null || asked.type !== "runtime_request.updated") return;
      assert.equal(asked.threadId, THREAD_ID);
      assert.equal(asked.runtimeRequest.nativeRequestRef?.nativeId, "ask-after-unreadable-state");
      assert.equal(asked.runtimeRequest.status, "pending");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("offers an explicit empty value for extension input dialogs", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-input",
        method: "input",
        title: "Optional value",
      });
      const event = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
      );
      assert.isTrue(
        event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
      );
      if (event.type !== "turn_item.updated" || event.turnItem.type !== "user_input_request")
        return;
      assert.equal(event.turnItem.questions[0]?.options[0]?.value, "");
      yield* runtime.respondToRuntimeRequest({
        requestId: event.turnItem.requestId,
        answers: { "ui-input": "" },
      });
      const response = yield* fake.takeRequest("extension_ui_response");
      assert.equal(response["value"], "");
      assert.isUndefined(response["cancelled"]);
      const resolved = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "resolved",
      );
      assert.equal(
        resolved.type === "runtime_request.updated" ? resolved.runtimeRequest.status : null,
        "resolved",
      );
      const node = yield* takeEvent((event) => event.type === "node.updated");
      assert.equal(node.type === "node.updated" ? node.node.status : null, "completed");
      const item = yield* takeEvent((event) => event.type === "turn_item.updated");
      assert.equal(item.type === "turn_item.updated" ? item.turnItem.status : null, "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { decision: "accept" as const, native: { confirmed: true }, itemStatus: "completed" as const },
    {
      decision: "decline" as const,
      native: { confirmed: false },
      itemStatus: "cancelled" as const,
    },
    { decision: "cancel" as const, native: { cancelled: true }, itemStatus: "cancelled" as const },
  ])("settles a confirm dialog with $decision as $itemStatus", ({ decision, native, itemStatus }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: `ui-confirm-${decision}`,
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .pi/extensions.",
      });
      const pending = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      const requestId =
        pending.type === "runtime_request.updated" ? pending.runtimeRequest.id : undefined;
      yield* runtime.respondToRuntimeRequest({ requestId: requestId!, decision });
      const uiResponse = yield* fake.takeRequest("extension_ui_response");
      assert.equal(uiResponse["id"], `ui-confirm-${decision}`);
      if ("confirmed" in native) {
        assert.equal(uiResponse["confirmed"], native.confirmed);
        assert.isUndefined(uiResponse["cancelled"]);
      } else {
        assert.equal(uiResponse["cancelled"], native.cancelled);
        assert.isUndefined(uiResponse["confirmed"]);
      }
      const resolved = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "resolved",
      );
      assert.equal(
        resolved.type === "runtime_request.updated" ? resolved.runtimeRequest.status : null,
        "resolved",
      );
      const node = yield* takeEvent((event) => event.type === "node.updated");
      assert.equal(node.type === "node.updated" ? node.node.status : null, itemStatus);
      const item = yield* takeEvent((event) => event.type === "turn_item.updated");
      assert.equal(item.type === "turn_item.updated" ? item.turnItem.status : null, itemStatus);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("raises bridge edit confirmations as file-change approvals", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      for (const [id, title, requestKind] of [
        ["ui-edit", "Allow edit?", "file-change"],
        ["ui-bash", "Allow bash?", "command"],
        ["ui-ext", "Deploy to staging?", "command"],
      ] as const) {
        yield* fake.emit({ type: "extension_ui_request", id, method: "confirm", title });
        const item = yield* takeEvent(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
        );
        assert.isTrue(
          item.type === "turn_item.updated" &&
            item.turnItem.type === "approval_request" &&
            item.turnItem.requestKind === requestKind,
          `${title} should be ${requestKind}`,
        );
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("reads a thread snapshot from pi's active branch", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.queueMessages({
        messages: [
          {
            role: "user",
            content: "hello pi",
            timestamp: 1700000000000,
          },
          {
            role: "assistant",
            content: [{ type: "text", text: "hello back" }],
            timestamp: 1700000001000,
          },
          { role: "toolResult", content: [] },
        ],
      });
      const snapshot = yield* runtime.readThreadSnapshot({ providerThread });
      assert.equal(snapshot.messages.length, 2);
      assert.equal(snapshot.messages[0]!.role, "user");
      assert.equal(snapshot.messages[0]!.text, "hello pi");
      assert.equal(snapshot.messages[1]!.role, "assistant");
      assert.equal(snapshot.messages[1]!.text, "hello back");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("passes a heads-up answer to the you-should-know command only", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const answer = runtime.answerHeadsUp!;
      const opening = fake.allRequests().length;
      const sent = () =>
        fake
          .allRequests()
          .slice(opening)
          .filter((record) => record["type"] === "prompt" || record["type"] === "get_commands")
          .map((record) => record["message"] ?? record["type"]);

      // A prompt template named ysk would go to the model, so it does not count.
      fake.queueCommands({ commands: [{ name: "ysk", source: "prompt" }] });
      yield* answer({ providerThread, noteId: "k3x9", resolution: "knew" });
      // A note from another native session was never in this process.
      yield* answer({
        providerThread: {
          ...providerThread,
          nativeThreadRef: {
            driver: PI_PROVIDER,
            nativeId: "/fake/other.jsonl",
            strength: "strong",
          },
        },
        noteId: "k3x9",
        resolution: "knew",
      });
      fake.queueCommands({ commands: [{ name: "ysk", source: "extension" }] });
      yield* answer({ providerThread, noteId: "k3x9", resolution: "dismiss" });

      assert.deepEqual(sent(), ["get_commands", "get_commands", "/ysk answer k3x9 dismiss"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps snapshot message identities distinct across native sessions", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const first = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const messages = {
        messages: [{ role: "user", content: "same text", timestamp: 1700000000000 }],
      };
      fake.queueMessages(messages);
      const a = yield* runtime.readThreadSnapshot({ providerThread: first });
      fake.queueState({ sessionFile: "/fake/another-session.jsonl" });
      const second = yield* runtime.ensureThread({
        threadId: ThreadId.make("second-thread"),
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: {
          ...first,
          nativeThreadRef: {
            driver: PI_PROVIDER,
            nativeId: "/fake/another-session.jsonl",
            strength: "strong",
          },
        },
      });
      fake.queueMessages(messages);
      const b = yield* runtime.readThreadSnapshot({ providerThread: second });
      assert.notEqual(a.messages[0]!.id, b.messages[0]!.id);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rejects a nonpersistent session UUID instead of treating it as a resumable path", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      // A --no-session Pi keeps its session in memory and reports no file.
      fake.queueState({ sessionFile: undefined });
      const result = yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        })
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a settled turn's late prompt rejection off the next turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      // An extension command can hold its prompt ack open past settlement.
      yield* startTurn(runtime, providerThread, "default", [], "/my-command");
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_settled" });
      const firstTerminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(firstTerminal.type === "turn.terminal" && firstTerminal.status === "completed");

      yield* startTurn(runtime, providerThread, "default", [], "Second turn", undefined, 2);
      yield* fake.takeRequest("prompt");
      // The rejection answers the first turn's prompt. It must not consume or
      // fail the second turn's prompt acknowledgement.
      yield* fake.emit({
        type: "response",
        command: "prompt",
        success: false,
        error: "late command rejection",
      });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_settled" });
      const secondTerminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(
        secondTerminal.type === "turn.terminal" && secondTerminal.status === "completed",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("starts a turn while an extension run is already streaming", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      const prompt = yield* fake.takeRequest("prompt");
      // An extension continuation resumed with the session and is streaming.
      // Pi rejects a prompt without streamingBehavior while it streams.
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(
        prompt["streamingBehavior"] === undefined
          ? {
              type: "response",
              command: "prompt",
              success: false,
              error:
                "Agent is already processing. Specify streamingBehavior ('steer' or 'followUp') to queue the message.",
            }
          : { type: "response", command: "prompt", success: true },
      );
      yield* fake.emit({ type: "agent_settled" });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows compaction progress and completes the same activity row", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "compaction_start", reason: "threshold" });

      const runningNode = yield* takeEvent(
        (event) => event.type === "node.updated" && event.node.kind === "system",
      );
      const runningItem = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      assert.isTrue(
        runningNode.type === "node.updated" &&
          runningNode.node.status === "running" &&
          runningItem.type === "turn_item.updated" &&
          runningItem.turnItem.type === "compaction" &&
          runningItem.turnItem.status === "running" &&
          runningItem.turnItem.title === "Compacting context...",
      );

      yield* fake.emit({
        type: "compaction_end",
        reason: "threshold",
        result: { summary: "smaller", tokensBefore: 200_000, estimatedTokensAfter: 3_400 },
        aborted: false,
        willRetry: false,
      });
      const completedNode = yield* takeEvent(
        (event) => event.type === "node.updated" && event.node.kind === "system",
      );
      const completedItem = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      assert.isTrue(
        runningNode.type === "node.updated" &&
          completedNode.type === "node.updated" &&
          runningItem.type === "turn_item.updated" &&
          runningItem.turnItem.type === "compaction" &&
          completedItem.type === "turn_item.updated" &&
          completedItem.turnItem.type === "compaction" &&
          completedNode.node.id === runningNode.node.id &&
          completedNode.node.status === "completed" &&
          completedItem.turnItem.id === runningItem.turnItem.id &&
          completedItem.turnItem.ordinal === runningItem.turnItem.ordinal &&
          completedItem.turnItem.startedAt === runningItem.turnItem.startedAt &&
          completedItem.turnItem.status === "completed" &&
          completedItem.turnItem.title === "Context compacted" &&
          completedItem.turnItem.beforeTokenCount === 200_000 &&
          completedItem.turnItem.afterTokenCount === 3_400,
      );

      yield* fake.emit({ type: "agent_settled" });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("uses distinct compaction IDs for first turns in separate threads", () =>
    Effect.gen(function* () {
      const firstFake = yield* makeFakePi;
      const { runtime: firstRuntime, takeEvent: takeFirstEvent } = yield* openRuntime(firstFake);
      const firstProviderThread = yield* firstRuntime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(firstRuntime, firstProviderThread);
      yield* firstFake.takeRequest("prompt");
      yield* firstFake.emit({ type: "agent_start" });
      yield* firstFake.emit({ type: "compaction_start", reason: "threshold" });
      const first = yield* takeFirstEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      const secondThreadId = ThreadId.make("thread-pi-test-second");
      const secondFake = yield* makeFakePi;
      const { runtime: secondRuntime, takeEvent: takeSecondEvent } = yield* openRuntime(
        secondFake,
        "default",
        secondThreadId,
        ProviderSessionId.make("provider-session-pi-test-second"),
      );
      const secondProviderThread = yield* secondRuntime.ensureThread({
        threadId: secondThreadId,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(
        secondRuntime,
        secondProviderThread,
        "default",
        [],
        "Hello from another thread",
        undefined,
        1,
        secondThreadId,
      );
      yield* secondFake.takeRequest("prompt");
      yield* secondFake.emit({ type: "agent_start" });
      yield* secondFake.emit({ type: "compaction_start", reason: "threshold" });
      const second = yield* takeSecondEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      assert.isTrue(
        first.type === "turn_item.updated" &&
          first.turnItem.type === "compaction" &&
          second.type === "turn_item.updated" &&
          second.turnItem.type === "compaction" &&
          first.turnItem.ordinal === second.turnItem.ordinal &&
          first.turnItem.id !== second.turnItem.id,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows aborted compactions as stopped", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      const running = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        aborted: true,
        willRetry: false,
      });
      const stopped = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      assert.isTrue(
        running.type === "turn_item.updated" &&
          running.turnItem.type === "compaction" &&
          stopped.type === "turn_item.updated" &&
          stopped.turnItem.type === "compaction" &&
          stopped.turnItem.id === running.turnItem.id &&
          stopped.turnItem.status === "cancelled" &&
          stopped.turnItem.title === "Context compaction stopped",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps the turn open and updates one retry row through final failure", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");

      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [], willRetry: true });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 3,
        delayMs: 3_000,
        errorMessage: "529 overloaded",
      });
      const firstRetry = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          firstRetry.turnItem.status === "running" &&
          firstRetry.turnItem.title === "Provider retry" &&
          firstRetry.turnItem.failure.retryable === true &&
          firstRetry.turnItem.retry?.attempt === 1 &&
          firstRetry.turnItem.retry.maxAttempts === 3 &&
          firstRetry.turnItem.retry.retryDelayMs === 3_000,
      );

      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 3,
        maxAttempts: 3,
        delayMs: 12_000,
        errorMessage: "529 still overloaded",
      });
      const lastRetry = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          lastRetry.type === "turn_item.updated" &&
          lastRetry.turnItem.type === "error" &&
          lastRetry.turnItem.id === firstRetry.turnItem.id &&
          lastRetry.turnItem.startedAt === firstRetry.turnItem.startedAt &&
          lastRetry.turnItem.retry?.attempt === 3,
      );

      yield* fake.emit({
        type: "auto_retry_end",
        success: false,
        attempt: 3,
        finalError: "529 overloaded",
      });
      const failedRetry = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.status === "failed",
      );
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          failedRetry.type === "turn_item.updated" &&
          failedRetry.turnItem.type === "error" &&
          failedRetry.turnItem.id === firstRetry.turnItem.id &&
          failedRetry.turnItem.title === "Provider error" &&
          failedRetry.turnItem.failure.retryable === false &&
          failedRetry.turnItem.retry?.attempt === 3 &&
          failedRetry.turnItem.retry.maxAttempts === 3,
      );
      yield* fake.emit({ type: "agent_settled" });

      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "failed");
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message.includes("overloaded") &&
          terminal.retry?.attempt === 3 &&
          terminal.retry.maxAttempts === 3 &&
          terminal.retryStartedAt === firstRetry.turnItem.startedAt,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("preserves exhausted retry failure through non-retrying compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 5,
        maxAttempts: 5,
        delayMs: 48_000,
        errorMessage: "socket timed out",
      });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* fake.emit({
        type: "auto_retry_end",
        success: false,
        attempt: 5,
        finalError: "socket timed out",
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.status === "failed",
      );
      yield* fake.emit({ type: "compaction_start", reason: "threshold" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      yield* fake.emit({
        type: "compaction_end",
        reason: "threshold",
        result: { summary: "smaller", tokensBefore: 200_000, estimatedTokensAfter: 3_400 },
        aborted: false,
        willRetry: false,
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.emit({ type: "agent_settled" });

      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(
        terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message === "socket timed out" &&
          terminal.retry?.attempt === 5 &&
          terminal.retry.maxAttempts === 5,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("marks retry progress recovered when Pi succeeds", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [],
          stopReason: "error",
          errorMessage: "temporary network failure",
        },
      });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 5,
        delayMs: 3_000,
        errorMessage: "temporary network failure",
      });
      const running = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* fake.emit({ type: "auto_retry_end", success: true, attempt: 1 });
      const recovered = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        running.type === "turn_item.updated" &&
          running.turnItem.type === "error" &&
          recovered.type === "turn_item.updated" &&
          recovered.turnItem.type === "error" &&
          recovered.turnItem.id === running.turnItem.id &&
          recovered.turnItem.status === "completed" &&
          recovered.turnItem.title === "Provider recovered",
      );

      yield* fake.emit({ type: "agent_settled" });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops active retry progress when the turn is interrupted", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const runningTurn = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        runningTurn.type === "provider_turn.updated" ? runningTurn.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);

      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 2,
        maxAttempts: 5,
        delayMs: 6_000,
        errorMessage: "temporary network failure",
      });
      const retrying = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      yield* fake.takeRequest("abort");
      yield* fake.emit({ type: "agent_settled" });

      const stopped = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        retrying.type === "turn_item.updated" &&
          retrying.turnItem.type === "error" &&
          stopped.type === "turn_item.updated" &&
          stopped.turnItem.type === "error" &&
          stopped.turnItem.id === retrying.turnItem.id &&
          stopped.turnItem.status === "interrupted" &&
          stopped.turnItem.title === "Provider retry stopped",
      );
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps extension-started compaction and recovery in the settled turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      // Extension ctx.compact() waits for this first settlement, then starts
      // compaction in a detached continuation.
      fake.queueState({ isStreaming: false, isCompacting: true, pendingMessageCount: 0 });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      fake.queueState({ isStreaming: true, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
        aborted: false,
        willRetry: false,
      });
      yield* fake.emit({ type: "agent_start" });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.takeRequest("get_state");

      fake.queueState({ isStreaming: false, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps working after a settle probe fails before detached compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      fake.failNextState();
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      fake.queueState({ isStreaming: false, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
        aborted: false,
        willRetry: false,
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("restarts Pi when Stop interrupts detached compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.equal(running.type, "provider_turn.updated");
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.emit({ type: "agent_start" });

      fake.queueState({ isStreaming: false, isCompacting: true, pendingMessageCount: 0 });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "abort"));
      yield* fake.closeStdout;
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ignores an idle snapshot made stale by a steer", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.equal(running.type, "provider_turn.updated");
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.emit({ type: "agent_start" });

      fake.deferNextState();
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run:thread-pi-test:1"),
        providerThread,
        providerTurnId: providerTurnId!,
        message: {
          messageId: "message:thread-pi-test:late-steer" as never,
          text: "Continue after settlement",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      yield* fake.takeRequest("prompt");
      yield* fake.resolveDeferredState({
        isStreaming: false,
        isCompacting: false,
        pendingMessageCount: 0,
      });

      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_end", contentIndex: 0, content: "Recovered" },
      });
      yield* fake.emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "Recovered" }],
          stopReason: "stop",
        },
      });
      const assistantItem = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "assistant_message" &&
          event.turnItem.streaming === false,
      );
      assert.isTrue(
        assistantItem.type === "turn_item.updated" &&
          assistantItem.turnItem.type === "assistant_message" &&
          assistantItem.turnItem.text === "Recovered",
      );

      fake.queueState({ isStreaming: false, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("PiRpc framing", () => {
  it.effect("reassembles records across chunk boundaries and strips CR", () =>
    Effect.gen(function* () {
      const stdout = yield* Queue.unbounded<Uint8Array>();
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(FAKE_PID),
            exitCode: Effect.never,
            isRunning: Effect.succeed(true),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.fromQueue(stdout),
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          }),
        ),
      );
      const connection = yield* makePiRpcConnection({
        command: "pi",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const push = (text: string) =>
        Queue.offer(stdout, new TextEncoder().encode(text)).pipe(Effect.asVoid);
      yield* push('{"type":"agent_');
      yield* push('start"}\r\n{"type":"agent_settled"}\nnot json\n{"type":"queue_update"}\n');

      yield* push("x".repeat(8 * 1024 * 1024));
      yield* push('x{"type":"must_not_emit"}\n{"type":"after_oversized"}\n');

      const first = yield* Queue.take(connection.events);
      assert.equal(first["type"], "agent_start");
      const second = yield* Queue.take(connection.events);
      assert.equal(second["type"], "agent_settled");
      const third = yield* Queue.take(connection.events);
      assert.equal(third["type"], "queue_update");
      assert.equal((yield* Queue.take(connection.events))["type"], "after_oversized");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

// This fails before a provider transcript exists, so a replay fixture is not
// an honest fit. The boundary is the stdio transport seeing stdout end.
describe("PiRpc response ownership", () => {
  it.effect(
    "discards late correlated prompt replies but delivers id-less prompt acknowledgements",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const connection = yield* makePiRpcConnection({
          command: "pi",
          args: ["--mode", "rpc"],
          cwd: undefined,
          env: {},
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner));
        fake.deferNextRequest("prompt");
        const probe = yield* Effect.forkChild(
          connection
            .request({ type: "prompt", message: "/t3-background-work" }, 2_000)
            .pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => null })),
        );
        const request = yield* fake.takeRequest("prompt");
        yield* TestClock.adjust(Duration.millis(2_000));
        assert.equal((yield* Fiber.join(probe))?._tag, "PiRpcTimeoutError");
        const lateReply = { type: "response", id: request.id, command: "prompt", success: true };
        yield* fake.emit(lateReply);
        yield* fake.emit(lateReply); // Duplicate/unmatched replies are not session events either.
        yield* fake.emit({ ...lateReply, id: 17 });
        const rejectedPrompt = {
          type: "response",
          command: "prompt",
          success: false,
          error: "new user prompt rejected",
        };
        yield* fake.emit(rejectedPrompt);
        yield* fake.emit({ type: "agent_start" });
        // The first event is the real user ack, not a stale probe ack that could
        // consume that user's adapter FIFO slot and hide this rejection.
        assert.deepEqual(yield* Queue.take(connection.events), rejectedPrompt);
        assert.deepEqual(yield* Queue.take(connection.events), { type: "agent_start" });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("PiRpc early process exit", () => {
  const makeHandle = (options: {
    readonly exitCode: Effect.Effect<ChildProcessSpawner.ExitCode>;
    readonly stderr: Stream.Stream<Uint8Array>;
  }) =>
    ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(FAKE_PID),
      exitCode: options.exitCode,
      isRunning: Effect.succeed(true),
      kill: () => Effect.void,
      unref: Effect.succeed(Effect.void),
      stdin: Sink.drain,
      stdout: Stream.empty,
      stderr: options.stderr,
      all: Stream.empty,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
    });

  it.effect("reports a nonzero exit code instead of an unexplained stdout close", () =>
    Effect.gen(function* () {
      const secret = "API_KEY=super-secret\n";
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          makeHandle({
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
            stderr: Stream.fromIterable([new TextEncoder().encode(secret)]),
          }),
        ),
      );
      const connection = yield* makePiRpcConnection({
        command: "pi",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const error = yield* Queue.take(connection.events).pipe(Effect.flip);
      assert.equal(error._tag, "PiRpcError");
      assert.equal(error.operation, "read");
      assert.equal(error.detail, "pi process exited with code 1");
      assert.isFalse((error.detail ?? "").includes("API_KEY"));
      assert.isFalse((error.detail ?? "").includes("super-secret"));
      assert.isFalse(error.message.includes("API_KEY"));
      assert.isFalse(error.message.includes("super-secret"));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the unexplained stdout-close message when the process has not exited", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          makeHandle({
            exitCode: Effect.never,
            stderr: Stream.empty,
          }),
        ),
      );
      const connection = yield* makePiRpcConnection({
        command: "pi",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const fiber = yield* Effect.forkChild(Queue.take(connection.events).pipe(Effect.flip));
      yield* TestClock.adjust(Duration.millis(300));
      const error = yield* Fiber.join(fiber);
      assert.equal(error._tag, "PiRpcError");
      assert.equal(error.operation, "read");
      assert.equal(error.detail, "pi process closed stdout");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the exit-code diagnosis when stdin breaks while exit is still pending", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(FAKE_PID),
            exitCode: Effect.sleep(Duration.millis(50)).pipe(
              Effect.andThen(Effect.succeed(ChildProcessSpawner.ExitCode(1))),
            ),
            isRunning: Effect.succeed(true),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.fail(
              PlatformError.systemError({
                _tag: "Unknown",
                module: "ChildProcess",
                method: "stdin",
                description: "broken pipe",
              }),
            ),
            stdout: Stream.empty,
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          }),
        ),
      );
      const connection = yield* makePiRpcConnection({
        command: "pi",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const fiber = yield* Effect.forkChild(Queue.take(connection.events).pipe(Effect.flip));
      yield* TestClock.adjust(Duration.millis(300));
      const error = yield* Fiber.join(fiber);
      assert.equal(error._tag, "PiRpcError");
      assert.equal(error.operation, "read");
      assert.equal(error.detail, "pi process exited with code 1");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
