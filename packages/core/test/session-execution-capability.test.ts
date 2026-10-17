import { describe, expect, spyOn } from "bun:test"
import { Effect, Layer } from "effect"
import { TestClock } from "effect/testing"
import { SessionExecutionCapability } from "@opencode/core/session/execution-capability"
import { SessionRunnerLLM } from "@opencode/core/session/runner/llm"
import { Session } from "@opencode/core/session"
import { SessionMessage } from "@opencode/core/session/message"
import { testEffect } from "./lib/effect"

const it = testEffect(Layer.empty)
const sessionID = Session.ID.make("ses_execution_capability")
const inputID = SessionMessage.ID.make("msg_execution_capability")

describe("Session execution capability", () => {
  it.effect("requires the in-memory grant matching a durable MCP capability marker", () =>
    Effect.gen(function* () {
      const missing = yield* SessionRunnerLLM.executionGrant(
        sessionID,
        [{ id: inputID, type: "user", capabilities: ["mcp"] }],
        undefined,
      ).pipe(Effect.flip)
      expect(missing._tag).toBe("Session.ExecutionCapabilityUnavailableError")

      const mismatched = yield* SessionRunnerLLM.executionGrant(
        sessionID,
        [{ id: inputID, type: "user", capabilities: ["mcp"] }],
        {
          inputID: SessionMessage.ID.make("msg_other_capability"),
          mcp: { tenant: { type: "remote", url: "https://mcp.example.test" } },
        },
      ).pipe(Effect.flip)
      expect(mismatched._tag).toBe("Session.ExecutionCapabilityUnavailableError")

      const grant = {
        inputID,
        mcp: {
          tenant: {
            type: "remote" as const,
            url: "https://mcp.example.test",
            headers: { authorization: "Bearer short-lived" },
          },
        },
      }
      expect(
        yield* SessionRunnerLLM.executionGrant(
          sessionID,
          [{ id: inputID, type: "user", capabilities: ["mcp"] }],
          grant,
        ),
      ).toBe(grant)
    }),
  )

  it.effect("does not apply a stale grant to an ordinary prompt", () =>
    Effect.gen(function* () {
      const grant = {
        inputID,
        mcp: { tenant: { type: "remote" as const, url: "https://mcp.example.test" } },
      }
      expect(
        yield* SessionRunnerLLM.executionGrant(
          sessionID,
          [{ id: SessionMessage.ID.make("msg_ordinary"), type: "user" }],
          grant,
        ),
      ).toBeUndefined()
    }),
  )
  it.effect("expired or lost claims never restore readiness and stale commits cannot replace a newer claim", () =>
    Effect.gen(function* () {
      const id = SessionMessage.ID.make("msg_claim_expiry")
      yield* SessionExecutionCapability.clear(sessionID)
      const grant = { inputID: id, mcp: {}, revision: 5 }
      yield* SessionExecutionCapability.set(sessionID, grant)
      const timers = spyOn(globalThis, "setTimeout")
      const expiry = yield* SessionExecutionCapability.beginRenewal(sessionID, id, "expired").pipe(
        Effect.map((receipt) => {
          expect(receipt).toBeDefined()
          return timers.mock.calls.find((call) => call[1] === 120_000)?.[0]
        }),
        Effect.ensuring(Effect.sync(() => timers.mockRestore())),
      )
      expect(yield* SessionExecutionCapability.ready(sessionID, id)).toBe(false)
      yield* TestClock.adjust("121 seconds")
      // Exercise the actual JavaScript timer callback as well as the Effect
      // deadline: advancing TestClock alone does not run setTimeout cleanup.
      if (typeof expiry !== "function") throw new Error("Missing renewal expiry callback")
      expiry()
      expect(yield* SessionExecutionCapability.set(sessionID, grant)).toBe(false)
      expect(yield* SessionExecutionCapability.commitRenewal(sessionID, grant, "expired")).toBe(false)
      expect(yield* SessionExecutionCapability.beginRenewal(sessionID, id, "expired")).toBeUndefined()
      expect(yield* SessionExecutionCapability.ready(sessionID, id)).toBe(false)
      expect(yield* SessionExecutionCapability.beginRenewal(sessionID, id, "fresh")).toBeDefined()
      expect(yield* SessionExecutionCapability.commitRenewal(sessionID, grant, "expired")).toBe(false)
      expect(yield* SessionExecutionCapability.commitRenewal(sessionID, { ...grant, revision: 4 }, "fresh")).toBe(false)
      expect(
        yield* SessionExecutionCapability.commitRenewal(
          sessionID,
          {
            ...grant,
            mcp: { tenant: { type: "remote", url: "https://changed.example.test" } },
          },
          "fresh",
        ),
      ).toBe(false)
      expect(yield* SessionExecutionCapability.commitRenewal(sessionID, grant, "fresh")).toBe(true)
      yield* SessionExecutionCapability.clear(sessionID) // native process-local state lost
      expect(yield* SessionExecutionCapability.ready(sessionID, id)).toBe(false)
      expect(yield* SessionExecutionCapability.committedRenewal(sessionID, grant, "fresh")).toBe(false)
    }),
  )
})
