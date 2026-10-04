import * as NodeModule from "node:module";
import * as NodeVM from "node:vm";
import { assert, describe, it } from "@effect/vitest";

import { PI_T3_MCP_EXTENSION_SOURCE } from "./piT3McpExtensionSource.ts";

type RequestHook = (
  event: { payload: unknown },
  ctx: { model: { provider: string } },
) => Record<string, unknown> | undefined;

async function loadRequestHook(): Promise<RequestHook> {
  const handlers = new Map<string, RequestHook>();
  // Execute the shipped extension with MCP disabled; this path needs no Typebox.
  const source = NodeModule.stripTypeScriptTypes(
    PI_T3_MCP_EXTENSION_SOURCE.replace('import { Type } from "typebox";', "").replace(
      "export default async function",
      "async function",
    ),
  );
  await NodeVM.runInNewContext(`${source}\nt3McpExtension(pi)`, {
    process: { env: {} },
    pi: {
      registerCommand() {},
      on: (name: string, handler: RequestHook) => handlers.set(name, handler),
    },
  });
  const hook = handlers.get("before_provider_request");
  assert.isDefined(hook);
  return hook!;
}

describe("Pi upstream output-budget workaround", () => {
  it.each(["max_tokens", "max_completion_tokens"])(
    "caps %s without changing the conversation or tools",
    async (key) => {
      const hook = await loadRequestHook();
      const payload = {
        model: "moonshotai/kimi-k2.6",
        messages: [{ role: "user", content: "hello" }],
        tools: [{ type: "function", function: { name: "read" } }],
        [key]: 231_969,
      };
      const result = hook({ payload }, { model: { provider: "openrouter" } });
      assert.equal(result?.[key], 32_768);
      assert.strictEqual(result?.messages, payload.messages);
      assert.strictEqual(result?.tools, payload.tools);
      assert.equal(result?.model, payload.model);
      assert.equal(payload[key], 231_969);
    },
  );

  it("preserves smaller budgets and other providers' payloads", async () => {
    const hook = await loadRequestHook();
    for (const payload of [{ max_tokens: 8192 }, { max_completion_tokens: 32_768 }, {}, null]) {
      assert.isUndefined(hook({ payload }, { model: { provider: "openrouter" } }));
    }
    assert.isUndefined(
      hook({ payload: { max_tokens: 231_969 } }, { model: { provider: "anthropic" } }),
    );
  });
});

it("reports native extension keepalives and clears the pin once work finishes", async () => {
  let handler: (() => Promise<void>) | undefined;
  const statuses: string[] = [];
  const context = NodeVM.createContext({
    process: { env: {} },
    pi: {
      on() {},
      registerCommand(
        _name: string,
        command: { handler: (_args: string, ctx: unknown) => Promise<void> },
      ) {
        handler = () =>
          command.handler("", {
            ui: { setStatus: (_key: string, text: string) => statuses.push(text) },
          });
      },
    },
  });
  const source = NodeModule.stripTypeScriptTypes(
    PI_T3_MCP_EXTENSION_SOURCE.replace('import { Type } from "typebox";', "").replace(
      "export default async function",
      "async function",
    ),
  );
  await NodeVM.runInContext(source + "\nt3McpExtension(pi)", context);
  assert.isDefined(handler);
  await handler!();
  NodeVM.runInContext(
    'globalThis[Symbol.for("pi-subagents/keepalive")] = new Set(["wake:1"])',
    context,
  );
  await handler!();
  NodeVM.runInContext(
    'globalThis[Symbol.for("pi-subagents/keepalive")].clear(); globalThis[Symbol.for("pi-subagents/runtime")] = { runningSubagents: new Map([["child", {}]]) }',
    context,
  );
  await handler!();
  NodeVM.runInContext(
    'globalThis[Symbol.for("pi-subagents/runtime")].runningSubagents.clear()',
    context,
  );
  await handler!();
  assert.deepEqual(statuses, ["idle", "pending", "pending", "idle"]);
});
