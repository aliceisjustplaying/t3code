import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as HeadsUp from "./HeadsUpExplainService.ts";

const encodeNote = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

it.effect("reuses stored explanations and forks the recorded Claude session for rewrites", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-explain-" });
    const binary = path.join(dir, "claude.mjs");
    yield* fs.writeFileString(
      binary,
      `#!${process.execPath}\nimport assert from 'node:assert/strict';
assert.deepEqual(process.argv.slice(2), ['-p', '--resume', 'native-session', '--fork-session', '--output-format', 'text', '--tools', '', '--disable-slash-commands', '--permission-mode', 'dontAsk']);
let prompt = ''; for await (const chunk of process.stdin) prompt += chunk;
assert.ok(prompt.includes('in simpler words')); assert.ok(prompt.includes('Cached explanation'));
process.stdout.write('A simpler explanation.');\n`,
    );
    yield* fs.chmod(binary, 0o755);
    const note = yield* encodeNote({
      session: "native-session",
      cwd: dir,
      line: "The queue is shared.",
      explanation: "Cached explanation",
    });
    yield* fs.writeFileString(path.join(dir, "note.json"), note);
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        const previous = {
          T3_YSK_NOTES_DIR: process.env.T3_YSK_NOTES_DIR,
          T3_YSK_CLAUDE_BIN: process.env.T3_YSK_CLAUDE_BIN,
        };
        process.env.T3_YSK_NOTES_DIR = dir;
        process.env.T3_YSK_CLAUDE_BIN = binary;
        return previous;
      }),
      (previous) =>
        Effect.sync(() => {
          for (const [key, value] of Object.entries(previous)) {
            if (value === undefined) delete process.env[key];
            else process.env[key] = value;
          }
        }),
    );
    const service = yield* HeadsUp.HeadsUpExplainService;
    assert.deepEqual(yield* service.explain({ noteId: "note" }), {
      markdown: "Cached explanation",
    });
    assert.deepEqual(
      yield* service.explain({
        noteId: "note",
        direction: "simpler_words",
        previous: "Cached explanation",
      }),
      { markdown: "A simpler explanation." },
    );
    const invalid = yield* service.explain({ noteId: "../outside" }).pipe(Effect.flip);
    assert.equal(invalid.message, "invalid note id");
  }).pipe(
    Effect.scoped,
    Effect.provide(HeadsUp.layer.pipe(Layer.provideMerge(NodeServices.layer))),
  ),
);
