import {
  CheckpointScopeId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as EventSink from "../EventSink.ts";

export const seedProviderRun = Effect.fn("seedProviderRun")(function* (
  key: string,
  status: "running" | "failed" = "running",
) {
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make(`thread:${key}`);
  const runId = RunId.make(`run:${key}`);
  const nodeId = NodeId.make(`node:${key}`);
  const attemptId = RunAttemptId.make(`attempt:${key}`);
  const providerThreadId = ProviderThreadId.make(`provider-thread:${key}`);
  const providerTurnId = ProviderTurnId.make(`provider-turn:${key}`);
  const providerSessionId = ProviderSessionId.make(`session:${key}`);
  const driver = ProviderDriverKind.make("pi");
  const providerInstanceId = ProviderInstanceId.make("pi");
  const modelSelection = { instanceId: providerInstanceId, model: "test-model" };
  const appThread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make(`project:${key}`),
    title: key,
    createdBy: "user",
    creationSource: "web",
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThreadId,
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
  const providerThread: OrchestrationV2ProviderThread = {
    id: providerThreadId,
    appThreadId: threadId,
    driver,
    providerInstanceId,
    providerSessionId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId: `native:${key}`, strength: "strong" },
    nativeConversationHeadRef: null,
    status: status === "failed" ? "idle" : "active",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const run: OrchestrationV2Run = {
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId,
    modelSelection,
    providerThreadId,
    userMessageId: MessageId.make(`message:${key}`),
    rootNodeId: nodeId,
    activeAttemptId: attemptId,
    status,
    requestedAt: now,
    startedAt: now,
    completedAt: status === "failed" ? now : null,
    checkpointId: null,
    contextHandoffId: null,
  };
  // At start, the attempt snapshot does not yet know the native turn id.
  const attempt: OrchestrationV2RunAttempt = {
    id: attemptId,
    runId,
    attemptOrdinal: 1,
    rootNodeId: nodeId,
    providerInstanceId,
    providerThreadId,
    providerTurnId: null,
    reason: "initial",
    status,
    startedAt: now,
    completedAt: run.completedAt,
  };
  const rootNode: OrchestrationV2ExecutionNode = {
    id: nodeId,
    threadId,
    runId,
    parentNodeId: null,
    rootNodeId: nodeId,
    kind: "root_turn",
    status,
    countsForRun: true,
    providerThreadId,
    providerTurnId,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: run.completedAt,
  };
  const checkpointScope: OrchestrationV2CheckpointScope = {
    id: CheckpointScopeId.make(`scope:${key}`),
    threadId,
    runId,
    nodeId,
    parentScopeId: null,
    providerThreadId,
    kind: "root_run",
    ordinalWithinParent: 1,
    advancesAppRunCount: true,
    cwd: process.cwd(),
    createdAt: now,
  };
  const turn: OrchestrationV2ProviderTurn = {
    id: providerTurnId,
    providerThreadId,
    nodeId,
    runAttemptId: attemptId,
    nativeTurnRef: { driver, nativeId: `native-turn:${key}`, strength: "strong" },
    ordinal: 1,
    status: "running",
    startedAt: now,
    completedAt: null,
    tokenUsage: { usedTokens: 123, updatedAt: DateTime.formatIso(now) },
  };
  const base = { threadId, providerInstanceId, occurredAt: now };
  yield* sink.write({
    events: [
      {
        ...base,
        id: EventId.make(`event:${key}:thread`),
        type: "thread.created",
        payload: appThread,
      },
      {
        ...base,
        id: EventId.make(`event:${key}:provider-thread`),
        type: "provider-thread.updated",
        payload: providerThread,
      },
      { ...base, id: EventId.make(`event:${key}:run`), type: "run.created", runId, payload: run },
      {
        ...base,
        id: EventId.make(`event:${key}:attempt`),
        type: "run-attempt.created",
        runId,
        payload: attempt,
      },
      {
        ...base,
        id: EventId.make(`event:${key}:node`),
        type: "node.updated",
        runId,
        nodeId,
        payload: rootNode,
      },
      {
        ...base,
        id: EventId.make(`event:${key}:turn`),
        type: "provider-turn.updated",
        runId,
        nodeId,
        payload: turn,
      },
    ],
  });
  return {
    appThread,
    run,
    attempt,
    rootNode,
    checkpointScope,
    providerThread,
    providerSessionId,
    turn,
  };
});
