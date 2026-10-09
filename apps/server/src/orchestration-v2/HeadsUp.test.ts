import { assert, describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  NodeId,
  MessageId,
  RunId,
  RunAttemptId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderSessionId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as EventSink from "./EventSink.ts";
import * as EffectWorker from "./EffectWorker.ts";
import {
  ProviderAdapterEnsureThreadError,
  ProviderAdapterResumeThreadError,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { applyOrchestrationV2ProjectionEvent } from "../../../../packages/client-runtime/src/state/orchestrationV2Projection.ts";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import { parseHeadsUpNotice } from "./HeadsUpNotice.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

describe("parseHeadsUpNotice", () => {
  it("reads a Pi notify note with evidence and an explanation", () => {
    expect(
      parseHeadsUpNotice(
        "[ysk:k3x9] Heads up · `vp test` skips the e2e suite (package.json scripts)\n\n**Two suites**\n\nRun both.",
      ),
    ).toEqual({
      noteId: "k3x9",
      tag: "Heads up",
      line: "`vp test` skips the e2e suite",
      evidence: "package.json scripts",
      explanation: "**Two suites**\n\nRun both.",
    });
  });

  it("strips a Claude hook prefix and leaves out absent parts", () => {
    expect(
      parseHeadsUpNotice("PostToolUse:Bash says: [ysk:a1] You should know · The cache is shared."),
    ).toEqual({ noteId: "a1", tag: "You should know", line: "The cache is shared." });
  });

  it("ignores other notices", () => {
    expect(parseHeadsUpNotice("PostToolUse:Bash says: formatted 3 files")).toBeUndefined();
    expect(parseHeadsUpNotice("Model fell back to Sonnet")).toBeUndefined();
  });
});

const instanceId = ProviderInstanceId.make("codex");
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for heads-up controls"),
} as ProviderAdapterV2Shape;
const database = SqlitePersistence.layerMemory;
const orchestratorLayer = Layer.mergeAll(
  database,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  EffectOutbox.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "heads-up" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);

it.effect("resolving a heads-up persists, survives a small window, and can be undone", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const threadId = ThreadId.make("thread:heads-up");
    const itemId = TurnItemId.make("item:heads-up");
    const providerThreadId = ProviderThreadId.make("provider-thread:heads-up");
    const now = yield* DateTime.now;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-heads-up"),
      threadId,
      projectId: ProjectId.make("project:heads-up"),
      title: "Heads up",
      modelSelection: { instanceId, model: "gpt-5.1-codex" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* projections.apply({
      id: EventId.make("seed-heads-up"),
      type: "turn-item.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: itemId,
        threadId,
        runId: null,
        nodeId: null,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 1,
        status: "completed",
        title: "Two suites are skipped on CI.",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "system_notice",
        message: "PostToolUse:Bash says: [ysk:n-1] Heads up · Two suites are skipped on CI.",
        headsUp: { noteId: "n-1", tag: "Heads up", line: "Two suites are skipped on CI." },
      },
    });
    const headsUpOf = Effect.gen(function* () {
      const projection = yield* projections.getThreadProjection(threadId);
      const item = projection.turnItems.find((candidate) => candidate.id === itemId);
      return item?.type === "system_notice" ? item.headsUp : undefined;
    });

    // Bury the note under newer items: a small chat window omits it, while durable inbox state remains.
    for (let index = 0; index < 5; index++) {
      yield* projections.apply({
        id: EventId.make(`filler-${index}`),
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: TurnItemId.make(`item:filler-${index}`),
          threadId,
          runId: null,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 2 + index,
          status: "completed",
          title: "Plain",
          startedAt: now,
          completedAt: now,
          updatedAt: now,
          type: "system_notice",
          message: "Plain",
        },
      });
    }
    const windowIds = Effect.map(
      projections.getThreadSnapshotWindow(threadId, { rowLimit: 2 }),
      (snapshot) => snapshot.projection.turnItems.map((item) => item.id),
    );
    assert.notInclude(yield* windowIds, itemId);

    yield* orchestrator.dispatch({
      type: "thread.heads-up.read",
      commandId: CommandId.make("read-note"),
      threadId,
      turnItemId: itemId,
    });
    const readNote = yield* headsUpOf;
    assert(readNote?.readAt !== undefined);
    assert.equal(readNote.resolution, undefined);
    assert.notInclude(yield* windowIds, itemId);
    assert.equal((yield* outbox.listByCommandId(CommandId.make("read-note"))).length, 0);

    yield* orchestrator.dispatch({
      type: "thread.heads-up.resolve",
      commandId: CommandId.make("resolve-knew"),
      threadId,
      turnItemId: itemId,
      resolution: "knew",
    });
    assert.equal((yield* headsUpOf)?.resolution, "knew");
    assert.notInclude(yield* windowIds, itemId);
    const answers = (commandId: string) =>
      Effect.map(outbox.listByCommandId(CommandId.make(commandId)), (effects) =>
        effects.map((effect) => effect.request),
      );
    assert.deepEqual(yield* answers("resolve-knew"), [
      { type: "provider-heads-up.answer", providerThreadId, noteId: "n-1", resolution: "knew" },
    ]);

    yield* orchestrator.dispatch({
      type: "thread.heads-up.resolve",
      commandId: CommandId.make("resolve-restore"),
      threadId,
      turnItemId: itemId,
      resolution: null,
    });
    assert.deepEqual(yield* answers("resolve-restore"), [
      { type: "provider-heads-up.answer", providerThreadId, noteId: "n-1", resolution: null },
    ]);
    const restored = yield* headsUpOf;
    assert.deepEqual(restored && { ...restored, readAt: undefined }, {
      readAt: undefined,
      noteId: "n-1",
      tag: "Heads up",
      line: "Two suites are skipped on CI.",
    });

    const missing = yield* Effect.exit(
      orchestrator.dispatch({
        type: "thread.heads-up.resolve",
        commandId: CommandId.make("resolve-missing"),
        threadId,
        turnItemId: TurnItemId.make("item:missing"),
        resolution: "dismiss",
      }),
    );
    assert.equal(missing._tag, "Failure");

    // A forwarded note is answered in the parent's UI but its extension lives
    // in the child's provider session, including when Undo restores the note.
    const sourceThreadId = ThreadId.make("thread:heads-up-child");
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-heads-up-child"),
      threadId: sourceThreadId,
      projectId: ProjectId.make("project:heads-up"),
      title: "Source child",
      modelSelection: { instanceId, model: "gpt-5.1-codex" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    const sourceItem = (yield* projections.getThreadProjection(threadId)).turnItems.find(
      (candidate) => candidate.id === itemId,
    );
    assert(sourceItem?.type === "system_notice" && sourceItem.headsUp !== undefined);
    yield* projections.apply({
      id: EventId.make("seed-forwarded-heads-up"),
      type: "turn-item.updated",
      threadId,
      occurredAt: now,
      payload: { ...sourceItem, headsUp: { ...sourceItem.headsUp, sourceThreadId } },
    });
    for (const resolution of ["knew", null] as const) {
      const commandId = CommandId.make(`answer-forwarded-${resolution}`);
      yield* orchestrator.dispatch({
        type: "thread.heads-up.resolve",
        commandId,
        threadId,
        turnItemId: itemId,
        resolution,
      });
      const effects = yield* outbox.listByCommandId(commandId);
      assert.equal(effects[0]?.threadId, sourceThreadId);
      assert.deepEqual(effects[0]?.request, {
        type: "provider-heads-up.answer",
        providerThreadId,
        noteId: "n-1",
        resolution,
      });
    }
    yield* orchestrator.dispatch({
      type: "thread.delete",
      commandId: CommandId.make("delete-heads-up-child"),
      threadId: sourceThreadId,
    });
    for (const resolution of ["dismiss", null] as const) {
      const commandId = CommandId.make(`answer-orphan-${resolution}`);
      yield* orchestrator.dispatch({
        type: "thread.heads-up.resolve",
        commandId,
        threadId,
        turnItemId: itemId,
        resolution,
      });
      assert.equal((yield* headsUpOf)?.resolution, resolution ?? undefined);
      assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
    }
  }).pipe(Effect.provide(orchestratorLayer)),
);

// Command fan-out owns this regression: SQL-only tests cannot detect a stale
// subscribed parent whose inbox action targets the deduplicated source row.
it.live("synchronizes every forwarded thread live and on replay with one source feedback", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const source = ThreadId.make("copy-source");
      const parent = ThreadId.make("copy-parent");
      const providerThreadId = ProviderThreadId.make("copy-session");
      const now = yield* DateTime.now;
      for (const threadId of [source, parent]) {
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create-${threadId}`),
          threadId,
          projectId: ProjectId.make("copies"),
          title: threadId,
          modelSelection: { instanceId, model: "gpt-5.1-codex" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
        for (const suffix of ["first", "repeat", "other-session", "other-source"]) {
          yield* projections.apply({
            id: EventId.make(`seed-${threadId}-${suffix}`),
            type: "turn-item.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: TurnItemId.make(`${threadId}-${suffix}`),
              threadId,
              runId: null,
              nodeId: null,
              providerThreadId:
                suffix === "other-session"
                  ? ProviderThreadId.make("other-session")
                  : providerThreadId,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 1,
              status: "completed",
              title: "Note",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              type: "system_notice",
              message: "Note",
              headsUp: {
                ...(suffix === "other-source"
                  ? { sourceThreadId: ThreadId.make("other-source") }
                  : threadId === parent
                    ? { sourceThreadId: source }
                    : {}),
                noteId: "shared",
                tag: "Heads up",
                line: "Shared note",
              },
            },
          });
        }
      }
      const initial = new Map<ThreadId, OrchestrationV2ThreadProjection>();
      const live = new Map<
        ThreadId,
        Fiber.Fiber<ReadonlyArray<OrchestrationV2StoredEvent>, Orchestrator.OrchestratorV2Error>
      >();
      const attach = yield* Deferred.make<void>();
      for (const threadId of [source, parent]) {
        initial.set(threadId, yield* projections.getThreadProjection(threadId));
        const ready = yield* Deferred.make<void>();
        // Hold replay at the existing create event. The command then lands
        // before live attachment, so delivery must bridge that race.
        const watcher = yield* orchestrator.streamStoredEventsFrom({ threadId }).pipe(
          Stream.tap((stored) =>
            stored.event.type === "thread.created"
              ? Deferred.succeed(ready, undefined).pipe(Effect.andThen(Deferred.await(attach)))
              : Effect.void,
          ),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkScoped,
        );
        live.set(threadId, watcher);
        yield* Deferred.await(ready);
      }
      let cursor = Math.max(
        yield* orchestrator.getThreadEventSequence(source),
        yield* orchestrator.getThreadEventSequence(parent),
      );
      for (const action of ["read", "dismiss", "knew", "restore"] as const) {
        const commandId = CommandId.make(`copy-${action}`);
        const resolution = action === "restore" || action === "read" ? null : action;
        const threadId = action === "knew" || action === "restore" ? parent : source;
        const command =
          action === "read"
            ? {
                type: "thread.heads-up.read" as const,
                commandId,
                threadId,
                turnItemId: TurnItemId.make(`${threadId}-first`),
              }
            : {
                type: "thread.heads-up.resolve" as const,
                commandId,
                threadId,
                turnItemId: TurnItemId.make(`${threadId}-first`),
                resolution,
              };
        const result = yield* orchestrator.dispatch(command);
        assert.deepEqual(
          result.storedEvents.map((stored) => stored.event.threadId).sort(),
          [source, parent].sort(),
        );
        const effects = yield* outbox.listByCommandId(commandId);
        assert.equal(effects.length, action === "read" ? 0 : 1);
        if (action !== "read") {
          assert.equal(effects[0]?.threadId, source);
          assert.deepEqual(effects[0]?.request, {
            type: "provider-heads-up.answer",
            providerThreadId,
            noteId: "shared",
            resolution,
          });
        }
        // Retrying a command must not duplicate either feedback or fan-out events.
        assert.deepEqual((yield* orchestrator.dispatch(command)).storedEvents, result.storedEvents);
        assert.equal((yield* outbox.listByCommandId(commandId)).length, effects.length);
        if (action === "read") yield* Deferred.succeed(attach, undefined);
        for (const copyThreadId of [source, parent]) {
          const replay = yield* orchestrator
            .streamStoredEventsFrom({ threadId: copyThreadId, afterSequence: cursor })
            .pipe(Stream.take(1), Stream.runCollect);
          if (action === "read")
            assert.deepEqual((yield* Fiber.join(live.get(copyThreadId)!)).slice(1), replay);
          const before = initial.get(copyThreadId)!;
          const after = applyOrchestrationV2ProjectionEvent(before, replay[0]!.event)!;
          const durable = yield* projections.getThreadProjection(copyThreadId);
          for (const suffix of ["first", "repeat", "other-session", "other-source"]) {
            const id = `${copyThreadId}-${suffix}`;
            const item = after.turnItems.find((item) => item.id === id);
            const persisted = durable.turnItems.find((item) => item.id === id);
            assert(item?.type === "system_notice" && persisted?.type === "system_notice");
            assert.deepEqual(item.headsUp, persisted.headsUp);
            if (suffix === "first" || suffix === "repeat") {
              assert.isDefined(item.headsUp?.readAt);
              assert.equal(item.headsUp?.resolution, resolution ?? undefined);
            } else {
              assert.isUndefined(item.headsUp?.readAt);
              assert.isUndefined(item.headsUp?.resolution);
            }
          }
          initial.set(copyThreadId, after);
        }
        cursor = result.sequence;
      }
    }),
  ).pipe(Effect.provide(orchestratorLayer)),
);

const makeFeedbackFixture = Effect.fnUntraced(function* (
  name: string,
  options: {
    readonly failFirstLoad?: boolean;
    readonly failLoads?: ReadonlyArray<number>;
    readonly answerGate?: {
      readonly entered: Deferred.Deferred<void>;
      readonly release: Deferred.Deferred<void>;
    };
  } = {},
) {
  let registrations = 0;
  let opens = 0;
  let active = false;
  const delivered: Array<string | null> = [];
  const pi = ProviderDriverKind.make("pi");
  const piInstance = ProviderInstanceId.make("pi");
  const threadId = ThreadId.make(name);
  const sessionId = ProviderSessionId.make(`${name}-session`);
  const itemId = TurnItemId.make(`${name}-notice`);
  const now = yield* DateTime.now;
  const providerThread = {
    id: ProviderThreadId.make(`${name}-native`),
    driver: pi,
    providerInstanceId: piInstance,
    providerSessionId: sessionId,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: {
      driver: pi,
      nativeId: "/tmp/heads-up-session.jsonl",
      strength: "strong" as const,
    },
    nativeConversationHeadRef: null,
    status: "idle" as const,
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const piAdapter: ProviderAdapterV2Shape = {
    ...adapter,
    instanceId: piInstance,
    driver: pi,
    openSession: () =>
      Effect.sync(() => {
        opens++;
        let loaded = false;
        const register = Effect.gen(function* () {
          registrations++;
          if (active) return yield* Effect.fail("registerThread rejected during active Pi turn");
          // Pi clears its app binding before a native switch that can fail.
          loaded = false;
          if (
            (options.failFirstLoad && registrations === 1) ||
            options.failLoads?.includes(registrations)
          )
            return yield* Effect.fail("switch_session cancelled");
          loaded = true;
          return providerThread;
        });
        return {
          instanceId: piInstance,
          driver: pi,
          providerSessionId: sessionId,
          providerSession: {
            id: sessionId,
            driver: pi,
            providerInstanceId: piInstance,
            status: "ready" as const,
            cwd: "/tmp",
            model: "default",
            capabilities: {
              ...CodexProviderCapabilitiesV2,
              sessions: {
                ...CodexProviderCapabilitiesV2.sessions,
                supportsMultipleProviderThreadsPerSession: false,
              },
            },
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.never,
          ensureThread: () =>
            register.pipe(
              Effect.mapError(
                (cause) => new ProviderAdapterEnsureThreadError({ driver: pi, threadId, cause }),
              ),
            ),
          resumeThread: () =>
            register.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterResumeThreadError({
                    driver: pi,
                    providerSessionId: sessionId,
                    providerThreadId: providerThread.id,
                    cause,
                  }),
              ),
            ),
          answerHeadsUp: ({ resolution }) =>
            Effect.gen(function* () {
              if (options.answerGate) {
                yield* Deferred.succeed(options.answerGate.entered, undefined);
                yield* Deferred.await(options.answerGate.release);
              }
              assert.isTrue(loaded, "Feedback must not reach an unsuccessfully loaded runtime");
              delivered.push(resolution);
            }),
          startTurn: () =>
            Effect.sync(() => {
              active = true;
            }),
          steerTurn: () => Effect.die("unused"),
          interruptTurn: () =>
            Effect.sync(() => {
              active = false;
            }),
          respondToRuntimeRequest: () => Effect.die("unused"),
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: () => Effect.die("unused"),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };
  const layer = Layer.mergeAll(
    database,
    ProjectionStore.layer.pipe(Layer.provide(database)),
    EffectOutbox.layer.pipe(Layer.provide(database)),
    ProviderReplayHarness.layerWithRegistry(
      { name },
      ProviderAdapterRegistry.layerFromAdapters([piAdapter]),
      { databaseLayer: database, runEffectWorker: false },
    ),
  );
  const seed = Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create-${name}`),
      threadId,
      projectId: ProjectId.make(`${name}-project`),
      title: "Feedback",
      modelSelection: { instanceId: piInstance, model: "default" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* sink.write({
      events: [
        {
          id: EventId.make(`${name}-provider-thread`),
          type: "provider-thread.updated",
          threadId,
          occurredAt: now,
          payload: providerThread,
        },
        {
          id: EventId.make(`${name}-notice`),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: itemId,
            threadId,
            runId: null,
            nodeId: null,
            providerThreadId: providerThread.id,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            status: "completed",
            title: "Feedback note",
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            type: "system_notice",
            message: "[ysk:n1] Heads up · Feedback note",
            headsUp: { noteId: "n1", tag: "Heads up", line: "Feedback note" },
          },
        },
      ],
    });
  });
  const feedback = (id: string, resolution: "dismiss" | null = "dismiss") => ({
    id,
    commandId: CommandId.make(id),
    threadId,
    request: {
      type: "provider-heads-up.answer" as const,
      providerThreadId: providerThread.id,
      noteId: "n1",
      resolution,
    },
  });
  return {
    layer,
    seed,
    feedback,
    threadId,
    sessionId,
    itemId,
    providerThread,
    delivered,
    counts: () => ({ opens, registrations }),
  };
});

it.effect("retries failed heads-up session restoration before settling feedback", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFeedbackFixture("restore-heads-up", { failFirstLoad: true });
    const { threadId, providerThread, delivered } = fixture;
    yield* Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      yield* fixture.seed;
      const commandId = CommandId.make("restore-feedback");
      yield* sink.writeWithEffects({
        events: [],
        effects: [
          {
            id: "restore-feedback-effect",
            commandId,
            threadId,
            request: {
              type: "provider-heads-up.answer",
              providerThreadId: providerThread.id,
              noteId: "n1",
              resolution: "dismiss",
            },
          },
        ],
      });
      assert.equal(yield* worker.drain(), 1);
      assert.deepEqual(delivered, []);
      assert.equal((yield* outbox.listByCommandId(commandId))[0]?.status, "pending");
      yield* TestClock.adjust("100 millis");
      assert.equal(yield* worker.drain(), 1);
      assert.deepEqual(delivered, ["dismiss"]);
      assert.equal((yield* outbox.listByCommandId(commandId))[0]?.status, "succeeded");
      assert.deepEqual(fixture.counts(), { opens: 1, registrations: 2 });
      // A loaded session answers again without switching away from its live thread.
      yield* sink.writeWithEffects({
        events: [],
        effects: [
          {
            id: "undo-feedback-effect",
            commandId,
            threadId,
            request: {
              type: "provider-heads-up.answer",
              providerThreadId: providerThread.id,
              noteId: "n1",
              resolution: null,
            },
          },
        ],
      });
      yield* worker.drain();
      assert.deepEqual(delivered, ["dismiss", null]);
      assert.deepEqual(fixture.counts(), { opens: 1, registrations: 2 });
    }).pipe(Effect.provide(fixture.layer));
  }).pipe(Effect.scoped),
);

it.effect.each([false, true])(
  "restores feedback after a successful load then failed resume and ensure (cross-thread: %s)",
  (crossThread) =>
    Effect.gen(function* () {
      const fixture = yield* makeFeedbackFixture(`failed-reload-feedback-${crossThread}`, {
        failLoads: [2, 3],
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const sink = yield* EventSink.EventSinkV2;
        yield* fixture.seed;
        const { thread } = yield* projections.getThreadRecords(fixture.threadId, []);
        const modelSelection = {
          ...thread.modelSelection,
          options: [{ id: "thinking", value: "low" }],
        };
        const runtimePolicy = {
          cwd: thread.worktreePath,
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
        };
        const runtime = yield* sessions.open({
          threadId: fixture.threadId,
          providerSessionId: fixture.sessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.resumeThread({
          threadId: fixture.threadId,
          providerThread: fixture.providerThread,
          modelSelection,
          runtimePolicy,
        });
        const targetThreadId = crossThread
          ? ThreadId.make(`${fixture.threadId}-replacement`)
          : fixture.threadId;
        if (crossThread) {
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`${targetThreadId}-create`),
            threadId: targetThreadId,
            projectId: thread.projectId,
            title: "Replacement",
            modelSelection,
            runtimeMode: thread.runtimeMode,
            interactionMode: thread.interactionMode,
            branch: null,
            worktreePath: null,
            createdBy: "user",
            creationSource: "web",
          });
        }
        const nextSelection = { ...modelSelection, options: [{ id: "thinking", value: "high" }] };
        const targetProviderThread = crossThread
          ? {
              ...fixture.providerThread,
              appThreadId: targetThreadId,
              nativeThreadRef: {
                ...fixture.providerThread.nativeThreadRef,
                nativeId: "/tmp/other.jsonl",
              },
            }
          : fixture.providerThread;
        assert.equal(
          (yield* Effect.exit(
            runtime.resumeThread({
              threadId: targetThreadId,
              providerThread: targetProviderThread,
              modelSelection: nextSelection,
              runtimePolicy,
            }),
          ))._tag,
          "Failure",
        );
        assert.equal(
          (yield* Effect.exit(
            runtime.ensureThread({
              threadId: targetThreadId,
              existingProviderThread: targetProviderThread,
              modelSelection: nextSelection,
              runtimePolicy,
            }),
          ))._tag,
          "Failure",
        );
        const feedback = fixture.feedback(`failed-reload-answer-${crossThread}`);
        yield* sink.writeWithEffects({ events: [], effects: [feedback] });
        yield* worker.drain();
        assert.deepEqual(fixture.delivered, ["dismiss"]);
        assert.deepEqual(fixture.counts(), { opens: 1, registrations: 4 });
        assert.equal((yield* outbox.listByCommandId(feedback.commandId))[0]?.status, "succeeded");
      }).pipe(Effect.provide(fixture.layer));
    }).pipe(Effect.scoped),
);

it.effect(
  "delivers reviewed feedback during an active Pi turn after next-turn thinking changes",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeFeedbackFixture("active-feedback");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        yield* fixture.seed;
        const { thread } = yield* projections.getThreadRecords(fixture.threadId, []);
        const modelSelection = {
          ...thread.modelSelection,
          options: [{ id: "thinking", value: "low" }],
        };
        const runtimePolicy = {
          cwd: thread.worktreePath,
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
        };
        const runtime = yield* sessions.open({
          threadId: fixture.threadId,
          providerSessionId: fixture.sessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.resumeThread({
          threadId: fixture.threadId,
          providerThread: fixture.providerThread,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.startTurn({
          appThread: thread,
          threadId: fixture.threadId,
          runId: RunId.make("active-feedback-run"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("active-feedback-attempt"),
          rootNodeId: NodeId.make("active-feedback-node"),
          providerThread: fixture.providerThread,
          message: {
            messageId: MessageId.make("active-feedback-message"),
            text: "Continue",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection,
          runtimePolicy,
        });
        const nextSelection = { ...modelSelection, options: [{ id: "thinking", value: "high" }] };
        yield* orchestrator.dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("active-feedback-configure"),
          threadId: fixture.threadId,
          modelSelection: nextSelection,
        });
        const commandId = CommandId.make("active-feedback-reviewed");
        yield* orchestrator.dispatch({
          type: "thread.heads-up.resolve",
          commandId,
          threadId: fixture.threadId,
          turnItemId: fixture.itemId,
          resolution: "dismiss",
        });
        yield* worker.drain();
        assert.deepEqual(fixture.delivered, ["dismiss"]);
        assert.deepEqual(fixture.counts(), { opens: 1, registrations: 1 });
        assert.equal((yield* outbox.listByCommandId(commandId))[0]?.status, "succeeded");
        assert.deepEqual(
          (yield* projections.getThreadRecords(fixture.threadId, [])).thread.modelSelection,
          nextSelection,
        );
        // Ordinary turn preparation remains config-sensitive and cannot reload
        // this adapter while its previous turn is active.
        assert.equal(
          (yield* Effect.exit(
            runtime.resumeThread({
              threadId: fixture.threadId,
              providerThread: fixture.providerThread,
              modelSelection: nextSelection,
              runtimePolicy,
            }),
          ))._tag,
          "Failure",
        );
      }).pipe(Effect.provide(fixture.layer));
    }).pipe(Effect.scoped),
);

it.effect(
  "restores feedback from bounded metadata even when unrelated transcript rows cannot decode",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeFeedbackFixture("metadata-feedback");
      yield* Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const sql = yield* SqlClient.SqlClient;
        yield* fixture.seed;
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* sql`INSERT INTO orchestration_v2_projection_messages
        (message_id, thread_id, run_id, node_id, role, streaming, created_at, updated_at, payload_json)
        VALUES ('obsolete-feedback-history', ${fixture.threadId}, NULL, NULL, 'assistant', 0, ${now}, ${now}, '{"obsolete":true}')`;
        assert.equal(
          (yield* Effect.exit(projections.getThreadProjection(fixture.threadId)))._tag,
          "Failure",
        );
        const feedback = fixture.feedback("metadata-feedback-answer");
        yield* sink.writeWithEffects({ events: [], effects: [feedback] });
        yield* worker.drain();
        assert.deepEqual(fixture.delivered, ["dismiss"]);
        assert.equal((yield* outbox.listByCommandId(feedback.commandId))[0]?.status, "succeeded");
      }).pipe(Effect.provide(fixture.layer));
    }).pipe(Effect.scoped),
);

it.effect("does not reopen a deleted feedback source when a stale effect executes", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFeedbackFixture("deleted-feedback");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      yield* fixture.seed;
      yield* orchestrator.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("delete-feedback-source"),
        threadId: fixture.threadId,
      });
      yield* worker.drain();
      // Bypass enqueue-time liveness deliberately: durable effects can already
      // exist when execution starts, so the executor must check too.
      const feedback = fixture.feedback("stale-feedback-answer");
      yield* sink.writeWithEffects({ events: [], effects: [feedback] });
      yield* worker.drain();
      assert.deepEqual(fixture.delivered, []);
      assert.deepEqual(fixture.counts(), { opens: 0, registrations: 0 });
      assert.equal((yield* outbox.listByCommandId(feedback.commandId))[0]?.status, "succeeded");
    }).pipe(Effect.provide(fixture.layer));
  }).pipe(Effect.scoped),
);

it.effect("deletion cancels running and queued feedback before either can finish delivery", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const fixture = yield* makeFeedbackFixture("feedback-deletion-race", {
      answerGate: { entered, release },
    });
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      yield* fixture.seed;
      const running = fixture.feedback("running-feedback-answer");
      const queued = fixture.feedback("queued-feedback-answer", null);
      yield* sink.writeWithEffects({ events: [], effects: [running, queued] });
      const execution = yield* worker.runOnce.pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      assert.equal((yield* outbox.listByCommandId(running.commandId))[0]?.status, "running");
      yield* orchestrator.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("delete-running-feedback-source"),
        threadId: fixture.threadId,
      });
      // Join before releasing the adapter: only committed cancellation can
      // unblock this worker, not an accidentally fast provider answer.
      yield* Fiber.join(execution);
      yield* Deferred.succeed(release, undefined);
      yield* worker.drain();
      assert.deepEqual(fixture.delivered, []);
      for (const feedback of [running, queued])
        assert.equal((yield* outbox.listByCommandId(feedback.commandId))[0]?.status, "cancelled");
      assert.isTrue(
        Option.isNone(
          yield* (yield* ProviderSessionManager.ProviderSessionManagerV2).get(fixture.sessionId),
        ),
      );
    }).pipe(Effect.provide(fixture.layer));
  }).pipe(Effect.scoped),
);
