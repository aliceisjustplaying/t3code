import * as NodeAssert from "node:assert/strict";
import * as NodeOS from "node:os";
import * as Path from "effect/Path";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import { pendingHeadsUps } from "../../../packages/client-runtime/src/headsUp.ts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Logger from "effect/Logger";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as ServerConfig from "../src/config.ts";
import * as IdAllocator from "../src/orchestration-v2/IdAllocator.ts";
import * as Orchestrator from "../src/orchestration-v2/Orchestrator.ts";
import * as EffectOutbox from "../src/orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../src/orchestration-v2/EffectWorker.ts";
import * as ProviderAdapterRegistry from "../src/orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderEventIngestor from "../src/orchestration-v2/ProviderEventIngestor.ts";
import * as ProjectionStore from "../src/orchestration-v2/ProjectionStore.ts";
import * as ThreadCommandExecutor from "../src/orchestration-v2/ThreadCommandExecutor.ts";
import { SqlitePersistenceMemory } from "../src/persistence/Layers/Sqlite.ts";
import { makePiAdapterV2 } from "../src/orchestration-v2/Adapters/PiAdapterV2.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";

// Opt-in integration check: real installed Pi + YSK, isolated state, no model calls.
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeKnowledge = Schema.decodeSync(
  Schema.fromJsonString(Schema.Struct({ known: Schema.Array(Schema.String) })),
);
const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-ysk-" });
  const extension =
    process.argv[2] ??
    path.join(
      NodeOS.homedir(),
      ".pi/agent/git/github.com/aliceisjustplaying/pi-you-should-know/extensions/you-should-know/index.ts",
    );
  NodeAssert.ok(yield* fs.exists(extension), `YSK extension missing: ${extension}`);
  const nativeSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  let input: ChildProcessSpawner.ChildProcessHandle["stdin"] | undefined;
  const spawner = ChildProcessSpawner.make((command) =>
    nativeSpawner.spawn(command).pipe(
      Effect.tap((child) =>
        Effect.sync(() => {
          input = child.stdin;
        }),
      ),
    ),
  );
  const instanceId = ProviderInstanceId.make("pi");
  const threadId = ThreadId.make("pi-ysk-smoke");
  const providerSessionId = ProviderSessionId.make("pi-ysk-smoke-session");
  const modelSelection = { instanceId, model: "default" };
  const runtimePolicy = {
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    cwd: root,
  };
  const adapter = makePiAdapterV2({
    instanceId,
    settings: {
      enabled: true,
      binaryPath: "pi",
      launchArgs: `--offline --no-skills --no-context-files --no-extensions -e '${extension}' --session-dir '${root}/sessions'`,
      customModels: [],
    },
    environment: { ...process.env, PI_CODING_AGENT_DIR: path.join(root, "agent") },
    spawner,
    fileSystem: fs,
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    serverConfig: yield* ServerConfig.ServerConfig,
  });
  const scenario = `pi-ysk-${path.basename(root)}`;
  yield* Effect.addFinalizer(() =>
    fs.readDirectory(NodeOS.tmpdir()).pipe(
      Effect.flatMap((names) =>
        Effect.forEach(
          names.filter((name) => name.startsWith(`t3-orchestration-v2-replay-${scenario}-`)),
          (name) => fs.remove(path.join(NodeOS.tmpdir(), name), { recursive: true, force: true }),
        ),
      ),
      Effect.asVoid,
      Effect.orDie,
    ),
  );
  const database = SqlitePersistenceMemory;
  const base = Layer.mergeAll(
    EffectOutbox.layer.pipe(Layer.provide(database)),
    IdAllocator.layer,
    ThreadCommandExecutor.layer,
    ProjectionStore.layer.pipe(Layer.provide(database)),
  );
  const runtime = makeOrchestratorV2ReplayLayerWithRegistry(
    { name: scenario, runtimePolicyOverride: { cwd: root } },
    ProviderAdapterRegistry.makeLayer([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  );
  const ingestion = ProviderEventIngestor.layer.pipe(Layer.provide(Layer.mergeAll(base, runtime)));
  yield* Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create"),
      threadId,
      projectId: ProjectId.make("pi-ysk-project"),
      title: "YSK smoke",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    const nativeSession = path.join(root, "sessions/session.jsonl");
    yield* fs.makeDirectory(path.dirname(nativeSession), { recursive: true });
    yield* fs.writeFileString(
      nativeSession,
      encodeJson({
        type: "session",
        version: 3,
        id: "00000000-0000-4000-8000-000000000001",
        timestamp: DateTime.formatIso(yield* DateTime.now),
        cwd: root,
      }) + "\n",
    );
    const processScope = yield* Scope.make();
    yield* Effect.addFinalizer(() => Scope.close(processScope, Exit.void));
    const session = yield* adapter
      .openSession({ threadId, providerSessionId, modelSelection, runtimePolicy })
      .pipe(Scope.provide(processScope));
    const gotNote = yield* Deferred.make<void>();
    yield* session.events.pipe(
      Stream.runForEach((event) =>
        ingestor
          .ingestNormalized({ providerSessionId, providerInstanceId: instanceId, threadId, event })
          .pipe(
            Effect.tap(() =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "system_notice" &&
              event.turnItem.message.includes("[ysk:")
                ? Deferred.succeed(gotNote, undefined)
                : Effect.void,
            ),
          ),
      ),
      Effect.forkIn(processScope),
    );
    const providerThread = yield* session.ensureThread({ threadId, modelSelection, runtimePolicy });
    yield* session.ensureThread({
      threadId,
      modelSelection,
      runtimePolicy,
      existingProviderThread: {
        ...providerThread,
        nativeThreadRef: {
          driver: providerThread.driver,
          strength: "strong",
          nativeId: nativeSession,
        },
      },
    });
    NodeAssert.equal(
      yield* session.hasPendingBackgroundWork!,
      false,
      "T3's bridge must load alongside YSK",
    );
    yield* Stream.make(
      new TextEncoder().encode(encodeJson({ type: "prompt", message: "/ysk test" }) + "\n"),
    ).pipe(Stream.run(input!));
    yield* Deferred.await(gotNote);
    const notes = pendingHeadsUps((yield* orchestrator.getThreadProjection(threadId)).turnItems);
    NodeAssert.equal(
      notes.length,
      1,
      "idle Pi notice must reach the actual T3 projection and band selector",
    );
    NodeAssert.ok(notes[0]!.explanation, "Pi supplies the explanation used by T3");
    const turnItemId = notes[0]!.turnItemId;

    // Release the original process before answering: the effect worker must restore it.
    yield* Scope.close(processScope, Exit.void);
    for (const resolution of ["knew", null] as const) {
      yield* orchestrator.dispatch({
        type: "thread.heads-up.resolve",
        commandId: CommandId.make(`answer-${resolution}`),
        threadId,
        turnItemId,
        resolution,
      });
      NodeAssert.equal(yield* worker.drain(), 1);
      NodeAssert.equal(
        (yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(
          CommandId.make(`answer-${resolution}`),
        ))[0]?.status,
        "succeeded",
      );
      const knowledge = decodeKnowledge(
        yield* fs.readFileString(path.join(root, "agent/you-should-know/state.json")),
      );
      NodeAssert.equal(
        knowledge.known.includes("This is a test note from /ysk test."),
        resolution === "knew",
      );
      NodeAssert.equal(
        pendingHeadsUps((yield* orchestrator.getThreadProjection(threadId)).turnItems).length,
        resolution === null ? 1 : 0,
      );
    }
  }).pipe(Effect.provide(Layer.mergeAll(base, runtime, ingestion)));
}).pipe(
  Effect.scoped,
  Effect.provide(
    Layer.mergeAll(
      NodeServices.layer,
      IdAllocator.layer,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-ysk-smoke-config-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    ),
  ),
);
await Effect.runPromise(
  program.pipe(Effect.timeout("30 seconds"), Effect.provide(Logger.layer([Logger.make(() => {})]))),
);

await Effect.runPromise(
  Effect.log(
    "PASS: real Pi idle note → T3 persisted band → process release → Knew → Pi persistence → Undo → band restored",
  ),
);
