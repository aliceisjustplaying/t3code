import { ORCHESTRATION_V2_WS_METHODS } from "@t3tools/contracts";
import type { EnvironmentId, ThreadId, TurnItemId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Crypto from "effect/Crypto";
import { AsyncResult, Atom } from "effect/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  readThreadHeadsUp,
  resolveThreadHeadsUp,
  type ResolveThreadHeadsUpInput,
} from "../operations/commands.ts";
import {
  createEnvironmentCommand,
  createEnvironmentRpcQueryAtomFamily,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";

/** Environment-wide data, independent of which thread projections are loaded. */
export function createHeadsUpInboxAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | Crypto.Crypto | R, E>,
) {
  const summary = createEnvironmentRpcSubscriptionAtomFamily(runtime, {
    label: "environment-data:heads-up-inbox:summary",
    tag: ORCHESTRATION_V2_WS_METHODS.subscribeHeadsUpInbox,
    idleTtlMs: 0,
  });
  const revision = Atom.family((environmentId: EnvironmentId) =>
    Atom.map(summary({ environmentId, input: {} }), (result) =>
      Option.map(AsyncResult.value(result), (value) => value.sequence).pipe(Option.getOrNull),
    ),
  );
  return {
    summary,
    page: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:heads-up-inbox:page",
      tag: ORCHESTRATION_V2_WS_METHODS.getHeadsUpInbox,
      refreshTrigger: ({ environmentId }) => revision(environmentId),
      idleTtlMs: 0,
      staleTimeMs: 0,
    }),
    read: createEnvironmentCommand(runtime, {
      label: "environment-data:heads-up-inbox:read",
      execute: (input: { readonly threadId: ThreadId; readonly turnItemId: TurnItemId }) =>
        readThreadHeadsUp(input),
    }),
    resolve: createEnvironmentCommand(runtime, {
      label: "environment-data:heads-up-inbox:resolve",
      execute: (input: ResolveThreadHeadsUpInput) => resolveThreadHeadsUp(input),
    }),
  };
}
