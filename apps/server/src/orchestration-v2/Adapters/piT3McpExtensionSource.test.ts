import * as NodeModule from "node:module";
import * as NodeVM from "node:vm";
import { assert, describe, it } from "@effect/vitest";

import { PI_T3_MCP_EXTENSION_SOURCE } from "./piT3McpExtensionSource.ts";

// The shipped extension as a plain script. Tests supply Typebox through the VM context.
const runnableSource = NodeModule.stripTypeScriptTypes(
  PI_T3_MCP_EXTENSION_SOURCE.replace('import { Type } from "typebox";', "").replace(
    "export default async function",
    "async function",
  ),
);

type RequestHook = (
  event: { payload: unknown },
  ctx: { model: { provider: string } },
) => Record<string, unknown> | undefined;

async function loadRequestHook(): Promise<RequestHook> {
  const handlers = new Map<string, RequestHook>();
  // Execute the shipped extension with MCP disabled; this path needs no Typebox.
  await NodeVM.runInNewContext(`${runnableSource}\nt3McpExtension(pi)`, {
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

type RegisteredTool = {
  readonly name: string;
  readonly execute: (
    toolCallId: string,
    params: unknown,
    signal?: AbortSignal,
  ) => Promise<{ readonly content: ReadonlyArray<unknown>; readonly isError?: boolean }>;
};

// Loads the extension against an MCP endpoint that answers tools/call with `callResult`.
async function loadMcpTool(callResult: unknown): Promise<RegisteredTool> {
  const tools: RegisteredTool[] = [];
  const fetch = async (_url: string, init: { body: string }) => {
    const { id, method } = JSON.parse(init.body) as { id?: number; method: string };
    const result =
      method === "tools/list"
        ? { tools: [{ name: "preview_snapshot", inputSchema: { type: "object" } }] }
        : method === "tools/call"
          ? callResult
          : {};
    return new Response(id === undefined ? "" : JSON.stringify({ jsonrpc: "2.0", id, result }), {
      headers: { "content-type": "application/json" },
    });
  };
  await NodeVM.runInNewContext(`${runnableSource}\nt3McpExtension(pi)`, {
    process: { env: { T3_MCP_URL: "http://t3.test/mcp", T3_MCP_BEARER_TOKEN: "token" } },
    fetch,
    AbortSignal,
    Type: { Unsafe: (schema: unknown) => schema },
    pi: {
      on: () => undefined,
      registerCommand: () => undefined,
      registerTool: (tool: RegisteredTool) => tools.push(tool),
    },
  });
  assert.deepEqual(
    tools.map((tool) => tool.name),
    ["mcp__t3-code__preview_snapshot"],
  );
  return tools[0]!;
}

describe("Pi MCP tool results", () => {
  const image = { type: "image", data: "iVBORw0KGgo=", mimeType: "image/png" };

  it("passes screenshot image blocks to Pi after the text", async () => {
    const tool = await loadMcpTool({
      isError: false,
      structuredContent: { url: "https://t3.codes" },
      content: [{ type: "text", text: '{"url":"https://t3.codes"}' }, image],
    });
    const result = await tool.execute("call-1", {});
    assert.equal(result.content.length, 2);
    assert.include(result.content[0], { type: "text" });
    assert.include((result.content[0] as { text: string }).text, '{"url":"https://t3.codes"}');
    assert.deepEqual(result.content[1], image);
    assert.isUndefined(result.isError);
  });

  it("keeps text-only results text-only", async () => {
    const tool = await loadMcpTool({ content: [{ type: "text", text: "done" }] });
    assert.deepEqual((await tool.execute("call-1", {})).content, [{ type: "text", text: "done" }]);
  });

  it("sends an image-only result as the image instead of its base64 JSON", async () => {
    const tool = await loadMcpTool({ content: [image] });
    assert.deepEqual((await tool.execute("call-1", {})).content, [image]);
  });

  it("drops malformed image blocks", async () => {
    const tool = await loadMcpTool({
      content: [
        { type: "text", text: "shot" },
        { type: "image", data: "iVBORw0KGgo=" },
      ],
    });
    assert.deepEqual((await tool.execute("call-1", {})).content, [{ type: "text", text: "shot" }]);
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
  assert.deepEqual(
    statuses.map((text) => JSON.parse(text)),
    [
      { pending: false, retained: false },
      { pending: true, retained: false },
      { pending: true, retained: false },
      { pending: false, retained: false },
    ],
  );
  // Only known registry entries may opt out of completion; foreign/old
  // keepalives and native children remain blocking beside a retained service.
  for (const [script, expected] of [
    [
      'globalThis[Symbol.for("pi-subagents/keepalive")] = new Set(["service"]); globalThis[Symbol.for("pi-wake/background-work")] = new Map([["service", { completionBlocking: false, retained: true }]])',
      { pending: false, retained: true },
    ],
    [
      'globalThis[Symbol.for("pi-subagents/keepalive")].add("legacy")',
      { pending: true, retained: true },
    ],
    [
      'globalThis[Symbol.for("pi-subagents/keepalive")].delete("legacy"); globalThis[Symbol.for("pi-subagents/runtime")].runningSubagents.set("child", {})',
      { pending: true, retained: true },
    ],
    [
      'globalThis[Symbol.for("pi-subagents/runtime")].runningSubagents.clear(); globalThis[Symbol.for("pi-subagents/keepalive")].add("held"); globalThis[Symbol.for("pi-wake/background-work")].set("held", { completionBlocking: true, retained: false })',
      { pending: true, retained: true },
    ],
    [
      'globalThis[Symbol.for("pi-subagents/keepalive")].clear()',
      { pending: false, retained: false },
    ],
  ] as const) {
    NodeVM.runInContext(script, context);
    await handler!();
    assert.deepEqual(JSON.parse(statuses.at(-1)!), expected);
  }
});
