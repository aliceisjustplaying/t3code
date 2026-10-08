import { describe, expect, it } from "vite-plus/test";
import {
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  NodeId,
  ProviderThreadId,
  ProviderSessionId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { applyOrchestrationV2ProjectionEvent } from "./orchestrationV2Projection.ts";

const now = DateTime.makeUnsafe("2026-06-20T00:00:00.000Z");
const threadId = ThreadId.make("thread-reducer");
const runId = RunId.make("run-reducer");
const run = {
  id: runId,
  threadId,
  ordinal: 1,
  providerInstanceId: ProviderInstanceId.make("codex"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
  providerThreadId: null,
  userMessageId: MessageId.make("message-reducer"),
  rootNodeId: null,
  activeAttemptId: null,
  status: "completed",
  requestedAt: now,
  startedAt: now,
  completedAt: now,
  checkpointId: null,
  contextHandoffId: null,
} satisfies OrchestrationV2Run;

function commandItem(
  id: string,
  output = "done",
  ordinal = 1,
): Extract<OrchestrationV2TurnItem, { type: "command_execution" }> {
  return {
    id: TurnItemId.make(id),
    threadId,
    runId,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed",
    title: null,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    type: "command_execution",
    input: "pwd",
    output,
    exitCode: 0,
  };
}
const emptyProjection = {
  thread: {
    id: threadId,
    projectId: ProjectId.make("project-reducer"),
    title: "Reducer",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  },
  runs: [],
  attempts: [],
  nodes: [],
  subagents: [],
  providerSessions: [],
  providerThreads: [],
  providerTurns: [],
  runtimeRequests: [],
  messages: [],
  plans: [],
  turnItems: [],
  checkpointScopes: [],
  checkpoints: [],
  contextHandoffs: [],
  contextTransfers: [],
  visibleTurnItems: [],
  updatedAt: now,
} as OrchestrationV2ThreadProjection;

describe("applyOrchestrationV2ProjectionEvent", () => {
  it("keeps live token usage when the terminal provider turn omits it", () => {
    const providerTurnId = ProviderTurnId.make("provider-turn-reducer");
    const running = {
      id: providerTurnId,
      providerThreadId: ProviderThreadId.make("provider-thread-reducer"),
      nodeId: NodeId.make("provider-node-reducer"),
      runAttemptId: null,
      nativeTurnRef: null,
      ordinal: 1,
      status: "running" as const,
      startedAt: now,
      completedAt: null,
      tokenUsage: {
        usedTokens: 50_000,
        maxTokens: 200_000,
        updatedAt: "2026-08-29T00:00:00.000Z",
      },
    };
    const projection = { ...emptyProjection, providerTurns: [running] };
    const event = {
      id: "event-provider-turn-terminal",
      type: "provider-turn.updated",
      threadId,
      driver: "codex",
      occurredAt: now,
      payload: {
        ...running,
        status: "completed",
        completedAt: now,
        tokenUsage: undefined,
      },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);

    expect(next?.providerTurns[0]?.status).toBe("completed");
    expect(next?.providerTurns[0]?.tokenUsage).toEqual(running.tokenUsage);
  });

  it("applies thread lifecycle payloads instead of leaving stale metadata", () => {
    const archivedAt = DateTime.makeUnsafe("2026-06-20T01:00:00.000Z");
    const event = {
      id: "event-archive",
      type: "thread.archived",
      threadId,
      occurredAt: archivedAt,
      payload: { ...emptyProjection.thread, archivedAt, updatedAt: archivedAt },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(emptyProjection, event);
    expect(next?.thread.archivedAt).toEqual(archivedAt);
    expect(next?.updatedAt).toEqual(archivedAt);
  });

  it("ignores events for another thread", () => {
    const event = {
      id: "event-other",
      type: "thread.deleted",
      threadId: ThreadId.make("thread-other"),
      occurredAt: now,
      payload: { ...emptyProjection.thread, id: ThreadId.make("thread-other"), deletedAt: now },
    } as OrchestrationV2DomainEvent;

    expect(applyOrchestrationV2ProjectionEvent(emptyProjection, event)).toBe(emptyProjection);
  });

  it("preserves visible row identity when run updates do not change membership", () => {
    const item = commandItem("item-stable");
    const visibleTurnItems = [
      {
        position: 0,
        visibility: "local" as const,
        sourceThreadId: threadId,
        sourceItemId: item.id,
        item,
      },
    ];
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [item],
      visibleTurnItems,
    };
    const event = {
      id: "event-run-update",
      type: "run.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: { ...run, status: "completed" },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems).toBe(visibleTurnItems);
    expect(next?.visibleTurnItems[0]).toBe(visibleTurnItems[0]);
  });

  it("replaces only the updated visible item when membership is unchanged", () => {
    const first = commandItem("item-first", "first");
    const second = commandItem("item-second", "second");
    const firstRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: first.id,
      item: first,
    };
    const secondRow = {
      position: 1,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: second.id,
      item: second,
    };
    const updated = commandItem("item-first", "streamed output");
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [first, second],
      visibleTurnItems: [firstRow, secondRow],
    };
    const event = {
      id: "event-item-update",
      type: "turn-item.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: updated,
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems).not.toBe(projection.visibleTurnItems);
    expect(next?.visibleTurnItems[0]).not.toBe(firstRow);
    expect(next?.visibleTurnItems[0]?.item).toBe(updated);
    expect(next?.visibleTurnItems[1]).toBe(secondRow);
  });

  it("inserts live turn items by authoritative ordinal", () => {
    const queuedFuture = commandItem("item-queued-future", "queued", 300);
    const activeAssistant = commandItem("item-active-assistant", "done", 201);
    const queuedRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: queuedFuture.id,
      item: queuedFuture,
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [queuedFuture],
      visibleTurnItems: [queuedRow],
    };
    const event = {
      id: "event-active-assistant",
      type: "turn-item.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: activeAssistant,
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems.map((row) => row.item.id)).toEqual([
      activeAssistant.id,
      queuedFuture.id,
    ]);
    expect(next?.visibleTurnItems.map((row) => row.position)).toEqual([0, 1]);
  });

  it("removes only hidden local items while preserving inherited rows", () => {
    const inherited = commandItem("item-inherited");
    const local = commandItem("item-local");
    const inheritedRow = {
      position: 0,
      visibility: "inherited" as const,
      sourceThreadId: ThreadId.make("thread-source"),
      sourceItemId: inherited.id,
      item: inherited,
    };
    const localRow = {
      position: 1,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: local.id,
      item: local,
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [local],
      visibleTurnItems: [inheritedRow, localRow],
    };
    const event = {
      id: "event-run-rollback",
      type: "run.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: { ...run, status: "rolled_back" },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems).toEqual([inheritedRow]);
    expect(next?.visibleTurnItems[0]).toBe(inheritedRow);
  });
});

it("does not scan every row against every run for a streaming item update", () => {
  let runReads = 0;
  const runs = Array.from({ length: 100 }, (_, index) => ({
    ...run,
    get id() {
      runReads++;
      return RunId.make(`run-${index}`);
    },
  }));
  const items = Array.from({ length: 1000 }, (_, index) =>
    commandItem(`item-${index}`, "before", index),
  );
  const projection = {
    ...emptyProjection,
    runs,
    turnItems: items,
    visibleTurnItems: items.map((item, position) => ({
      item,
      position,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: item.id,
    })),
  };
  const payload = commandItem("item-999", "after", 999);
  const next = applyOrchestrationV2ProjectionEvent(projection, {
    id: "stream-update",
    type: "turn-item.updated",
    threadId,
    occurredAt: now,
    payload,
  } as OrchestrationV2DomainEvent);
  expect(next?.visibleTurnItems.at(-1)?.item).toBe(payload);
  expect(next?.visibleTurnItems[0]).toBe(projection.visibleTurnItems[0]);
  expect(runReads).toBeLessThanOrEqual(100);
});

it.each([undefined, ThreadId.make("child-thread")])(
  "does not expand a partial transcript for an inbox notice (source %s)",
  (sourceThreadId) => {
    const recent = commandItem("recent", "done", 100);
    const projection = {
      ...emptyProjection,
      turnItems: [recent],
      visibleTurnItems: [
        {
          position: 0,
          visibility: "local" as const,
          sourceThreadId: threadId,
          sourceItemId: recent.id,
          item: recent,
        },
      ],
    };
    const restored: OrchestrationV2TurnItem = {
      ...commandItem("old-note", "", 1),
      type: "system_notice",
      message: "Heads up",
      headsUp: {
        noteId: "old-note",
        tag: "Heads up",
        line: "Keep this note",
        ...(sourceThreadId ? { sourceThreadId } : {}),
      },
    };
    const next = applyOrchestrationV2ProjectionEvent(
      projection,
      {
        id: "undo-note",
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload: restored,
      } as OrchestrationV2DomainEvent,
      { partialTimeline: true, latestLocalTurnOrdinal: 100 },
    );
    expect(next?.turnItems).toEqual(projection.turnItems);
    // Inbox state is fetched independently of the paged transcript.
    expect(next?.visibleTurnItems).toEqual(projection.visibleTurnItems);
  },
);

it("updates loaded heads-up rows through review, read and restore without changing the paged window", () => {
  const source = ThreadId.make("child-source");
  const providerThreadId = ProviderThreadId.make("note-provider");
  const note: OrchestrationV2TurnItem = {
    ...commandItem("note", "", 1),
    providerThreadId,
    type: "system_notice",
    message: "Heads up",
    headsUp: { noteId: "note", tag: "Heads up", line: "Check cache", sourceThreadId: source },
  };
  const unrelated = commandItem("recent", "done", 100);
  const outsideWindow = { ...note, id: TurnItemId.make("outside-window") };
  const visibleOnly = { ...note, id: TurnItemId.make("inherited-note") };
  const rows = [unrelated, note, visibleOnly].map((item, position) => ({
    position: position + 40,
    visibility: position === 2 ? ("inherited" as const) : ("local" as const),
    sourceThreadId: position === 2 ? source : threadId,
    sourceItemId: item.id,
    item,
  }));
  let projection: OrchestrationV2ThreadProjection = {
    ...emptyProjection,
    turnItems: [note, outsideWindow, unrelated],
    visibleTurnItems: rows,
  };
  const readAt = "2026-06-20T01:00:00.000Z";
  for (const patch of [{ resolution: "knew" as const }, { readAt }, { resolution: null }]) {
    projection = applyOrchestrationV2ProjectionEvent(
      projection,
      {
        id: EventId.make("note-update"),
        type: "thread.heads-up.updated",
        threadId,
        occurredAt: now,
        payload: { sourceThreadId: source, providerThreadId, noteId: "note", ...patch },
      } satisfies OrchestrationV2DomainEvent,
      { partialTimeline: true, latestLocalTurnOrdinal: 100 },
    )!;
    const expected =
      "resolution" in patch && patch.resolution === null
        ? { ...note.headsUp, readAt }
        : { ...note.headsUp, resolution: "knew", ...("readAt" in patch ? { readAt } : {}) };
    for (const item of [
      projection.turnItems[0],
      projection.turnItems[1],
      ...projection.visibleTurnItems.slice(1).map((row) => row.item),
    ]) {
      expect(item).toMatchObject({ headsUp: expected });
      if (patch.resolution === null) expect(item).not.toHaveProperty("headsUp.resolution");
    }
    expect(projection.visibleTurnItems.map(({ item, ...row }) => row)).toEqual(
      rows.map(({ item, ...row }) => row),
    );
    expect(projection.visibleTurnItems[0]).toBe(rows[0]);
    expect(projection.turnItems[2]).toBe(unrelated);
  }
});

it("retains old live Stop targets and their completion without importing missing finished history", () => {
  const recent = commandItem("recent-job-history", "done", 100);
  const projection = {
    ...emptyProjection,
    turnItems: [recent],
    visibleTurnItems: [
      {
        position: 0,
        visibility: "local" as const,
        sourceThreadId: threadId,
        sourceItemId: recent.id,
        item: recent,
      },
    ],
  };
  const active: OrchestrationV2TurnItem = {
    ...commandItem("old-live-job", "", 1),
    type: "system_notice",
    message: "",
    status: "running",
    completedAt: null,
    job: {
      version: 1,
      scope: "live-runtime",
      id: "1",
      name: "Build",
      command: "make",
      cwd: "/tmp",
      state: "running",
      startedAt: 1,
      endedAt: null,
      exitCode: null,
      signal: null,
      output: "",
      outputOmitted: true,
      providerSessionId: ProviderSessionId.make("job-session"),
    },
  };
  const options = { partialTimeline: true, latestLocalTurnOrdinal: 100 };
  const event = (payload: OrchestrationV2TurnItem): OrchestrationV2DomainEvent => ({
    id: EventId.make("job-reducer"),
    type: "turn-item.updated",
    threadId,
    occurredAt: now,
    payload,
  });
  const finished: OrchestrationV2TurnItem = {
    ...active,
    status: "completed",
    completedAt: now,
    job: { ...active.job!, state: "succeeded", exitCode: 0, endedAt: 2 },
  };
  expect(
    applyOrchestrationV2ProjectionEvent(projection, event(finished), options)?.turnItems,
  ).toEqual([recent]);
  const running = applyOrchestrationV2ProjectionEvent(projection, event(active), options)!;
  expect(running.turnItems.some((item) => item.id === active.id)).toBe(true);
  const done = applyOrchestrationV2ProjectionEvent(running, event(finished), options)!;
  expect(done.turnItems.find((item) => item.id === active.id)).toMatchObject({
    job: { state: "succeeded" },
  });
});
