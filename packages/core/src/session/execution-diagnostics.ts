export * as SessionExecutionDiagnostics from "./execution-diagnostics.js"

import { PublicationAdmissionError } from "./error.js"
import { SessionEvent } from "@opencode/schema/session-event"
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

const eventTypes = new Set<string>(SessionEvent.Definitions.map((definition) => definition.type))

// This process boundary contains no provider messages, URLs, arguments, or
// credentials. A parent can capture failures without enabling raw debug logs.
function record(
  kind: "failed" | "retry",
  sessionID: SessionSchema.ID,
  error: SessionError.Error,
  inputID?: SessionMessage.ID,
  details: Record<string, string | number> = {},
) {
  if (!/^ses_[A-Za-z0-9_-]{1,124}$/.test(sessionID)) return undefined
  return (
    `opencode.execution.${kind} ` +
    JSON.stringify({
      version: 1,
      ...details,
      sessionID,
      ...(inputID !== undefined && /^msg_[A-Za-z0-9_-]{1,124}$/.test(inputID) ? { inputID } : {}),
      category: categories.has(error.type) ? error.type : "unknown",
      ...(error.status !== undefined && Number.isInteger(error.status) && error.status >= 100 && error.status <= 599
        ? { status: error.status }
        : {}),
    })
  )
}

export function format(
  sessionID: SessionSchema.ID,
  error: SessionError.Error,
  inputID?: SessionMessage.ID,
  cause?: unknown,
) {
  return record(
    "failed",
    sessionID,
    error,
    inputID,
    cause instanceof PublicationAdmissionError && eventTypes.has(cause.eventType)
      ? { site: "session/publication-admission.ts:assert", reason: cause.reason, eventType: cause.eventType }
      : {},
  )
}

export function formatRetry(
  sessionID: SessionSchema.ID,
  error: SessionError.Error,
  attempt: number,
  inputID?: SessionMessage.ID,
) {
  if (!Number.isSafeInteger(attempt) || attempt < 1 || attempt > 11) return undefined
  return record("retry", sessionID, error, inputID, { site: "session/runner/retry.ts:wait", attempt })
}

function write(line: string | undefined) {
  if (process.env.OPENCODE_EXECUTION_DIAGNOSTICS !== "1" || line === undefined) return
  try {
    process.stderr.write(line + "\n")
  } catch {
    // An unavailable diagnostic sink must not replace the execution's outcome.
  }
}

export function report(
  sessionID: SessionSchema.ID,
  error: SessionError.Error,
  inputID?: SessionMessage.ID,
  cause?: unknown,
) {
  write(format(sessionID, error, inputID, cause))
}

export function reportRetry(
  sessionID: SessionSchema.ID,
  error: SessionError.Error,
  attempt: number,
  inputID?: SessionMessage.ID,
) {
  write(formatRetry(sessionID, error, attempt, inputID))
}
