export * as SessionContextPolicy from "./session-context-policy.js"

import { Schema } from "effect"

// Version one selects request-only instructions, an actually empty tool
// capability set, witnessed history and required publication. Absence preserves
// legacy behavior. The policy is fixed by first session creation.
export interface Policy extends Schema.Schema.Type<typeof Policy> {}
export const Policy = Schema.Struct({ version: Schema.Literal(1) }).annotate({
  identifier: "Session.ContextPolicy",
})
