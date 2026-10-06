import { createHeadsUpInboxAtoms } from "@t3tools/client-runtime/state/heads-up-inbox";
import { connectionAtomRuntime } from "../connection/runtime";

export const headsUpInbox = createHeadsUpInboxAtoms(connectionAtomRuntime);
