export * as SessionExecutionDiagnostics from "./execution-diagnostics.js"

import type { SessionError } from "@opencode/schema/session-error"
import type { SessionSchema } from "./schema.js"
import type { SessionMessage } from "./message.js"

const categories = new Set([
  "provider.auth",
  "provider.no-route",
  "provider.rate-limit",
  "provider.quota",
  "provider.content-filter",
  "provider.transport",
  "provider.internal",
  "provider.invalid-output",
  "provider.invalid-request",
  "provider.unsupported-operation",
  "provider.unknown",
  "provider.timeout",
  "authorization",
  "permission.rejected",
  "tool.execution",
  "unknown",
])

// This process boundary contains no provider messages, URLs, arguments, or
// credentials. A parent can capture failures without enabling raw debug logs.
export function format(sessionID: SessionSchema.ID, error: SessionError.Error, inputID?: SessionMessage.ID) {
  if (!/^ses_[A-Za-z0-9_-]{1,124}$/.test(sessionID)) return undefined
  return (
    "opencode.execution.failed " +
    JSON.stringify({
      version: 1,
      sessionID,
      ...(inputID !== undefined && /^msg_[A-Za-z0-9_-]{1,124}$/.test(inputID) ? { inputID } : {}),
      category: categories.has(error.type) ? error.type : "unknown",
      ...(error.status !== undefined && Number.isInteger(error.status) && error.status >= 100 && error.status <= 599
        ? { status: error.status }
        : {}),
    })
  )
}

export function report(sessionID: SessionSchema.ID, error: SessionError.Error, inputID?: SessionMessage.ID) {
  if (process.env.OPENCODE_EXECUTION_DIAGNOSTICS !== "1") return
  const line = format(sessionID, error, inputID)
  if (line === undefined) return
  try {
    process.stderr.write(line + "\n")
  } catch {
    // An unavailable diagnostic sink must not replace the execution's outcome.
  }
}
