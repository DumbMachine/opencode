import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
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
})
