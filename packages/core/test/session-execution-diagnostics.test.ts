import { describe, expect, test } from "bun:test"
import { SessionExecutionDiagnostics } from "@opencode/core/session/execution-diagnostics"
import { PublicationAdmissionError } from "@opencode/core/session/error"
import { toSessionError } from "@opencode/core/session/to-session-error"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"

const sessionID = Session.ID.make("ses_diagnostics")
const inputID = SessionMessage.ID.make("msg_diagnostics")
const decode = (line: string | undefined) => JSON.parse(line!.slice(line!.indexOf(" ") + 1))

describe("Bounded execution diagnostics", () => {
  test("retains native publication rejection location, reason and canonical event", () => {
    for (const reason of ["service-missing", "event-rejected"] as const) {
      const cause = new PublicationAdmissionError({ eventType: "session.retry.scheduled", reason })
      expect(toSessionError(cause).type).toBe("authorization")
      expect(decode(SessionExecutionDiagnostics.format(sessionID, toSessionError(cause), inputID, cause))).toEqual({
        version: 1,
        sessionID,
        inputID,
        category: "authorization",
        site: "session/publication-admission.ts:assert",
        reason,
        eventType: "session.retry.scheduled",
      })
    }
  })

  test("logs retry cause and status without claiming execution failure or retaining provider content", () => {
    const error = { type: "provider.internal", status: 503, message: "Sensitive provider request and credential" }
    const line = SessionExecutionDiagnostics.formatRetry(sessionID, error, 2, inputID)
    expect(line).toStartWith("opencode.execution.retry ")
    expect(decode(line)).toEqual({
      version: 1,
      sessionID,
      inputID,
      category: "provider.internal",
      status: 503,
      site: "session/runner/retry.ts:wait",
      attempt: 2,
    })
    expect(line).not.toContain("Sensitive")
    for (const attempt of [0, 12, NaN, 1.5])
      expect(SessionExecutionDiagnostics.formatRetry(sessionID, error, attempt, inputID)).toBeUndefined()
  })

  test("does not serialize arbitrary causes or noncanonical event identifiers", () => {
    const error = { type: "unknown", message: "Sensitive raw detail" }
    for (const cause of [
      new Error("Sensitive raw detail"),
      new PublicationAdmissionError({ reason: "event-rejected", eventType: "session.secret-credential" }),
    ]) {
      expect(decode(SessionExecutionDiagnostics.format(sessionID, error, inputID, cause))).toEqual({
        version: 1,
        sessionID,
        inputID,
        category: "unknown",
      })
    }
  })
})
