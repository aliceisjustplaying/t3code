import { assert, describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import { parseHeadsUpNotice } from "./HeadsUpNotice.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
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
  }).pipe(Effect.provide(orchestratorLayer)),
);
