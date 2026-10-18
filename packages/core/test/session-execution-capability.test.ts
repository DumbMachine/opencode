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

describe("Disclosure execution grants", () => {
  const disclosure = {
    version: 1 as const,
    endpoint: "https://host.example.test/context",
    token: "private-credential",
    issuer: "host",
    audience: "native",
    keys: [{ id: "key", algorithm: "Ed25519" as const, publicKey: "a".repeat(43) }],
  }
  it.effect("requires every durable component and detaches installed and captured configuration", () =>
    Effect.gen(function* () {
      yield* SessionExecutionCapability.clear(sessionID)
      const required = ["mcp", "disclosure"]
      yield* SessionExecutionCapability.set(sessionID, { inputID, mcp: {}, revision: 1 })
      expect(yield* SessionExecutionCapability.ready(sessionID, inputID, required)).toBe(false)
      expect(
        (yield* SessionRunnerLLM.executionGrant(sessionID, [{ id: inputID, type: "user", capabilities: required }], {
          inputID,
          mcp: {},
          revision: 1,
        }).pipe(Effect.flip))._tag,
      ).toBe("Session.ExecutionCapabilityUnavailableError")
      const config = structuredClone(disclosure)
      expect(
        yield* SessionExecutionCapability.set(sessionID, { inputID, mcp: {}, disclosure: config, revision: 2 }),
      ).toBe(true)
      config.token = "mutated-after-install"
      const captured = yield* SessionExecutionCapability.get(sessionID, inputID)
      expect(captured?.disclosure?.token).toBe("private-credential")
      expect(yield* SessionExecutionCapability.ready(sessionID, inputID, required)).toBe(true)
      expect(yield* SessionExecutionCapability.set(sessionID, { inputID, mcp: {}, disclosure, revision: 2 })).toBe(true)
      expect(
        yield* SessionExecutionCapability.set(sessionID, { inputID, mcp: {}, disclosure: config, revision: 2 }),
      ).toBe(false)
      expect(yield* SessionExecutionCapability.set(sessionID, { inputID, mcp: {}, revision: 2 })).toBe(false)
      yield* SessionExecutionCapability.clear(sessionID)
      expect(yield* SessionExecutionCapability.ready(sessionID, inputID, required)).toBe(false)
    }),
  )
  it.effect("binds renewal fingerprints to disclosure settings and keeps lost-response witnesses credential-free", () =>
    Effect.gen(function* () {
      yield* SessionExecutionCapability.clear(sessionID)
      const original = { inputID, mcp: {}, disclosure, revision: 4 }
      expect(yield* SessionExecutionCapability.set(sessionID, original)).toBe(true)
      const claim = yield* SessionExecutionCapability.beginRenewal(sessionID, inputID, "disclosure-renewal")
      expect(claim).toBeDefined()
      expect(yield* SessionExecutionCapability.ready(sessionID, inputID, ["disclosure"])).toBe(false)
      const changed = { ...original, disclosure: { ...disclosure, token: "renewed-private" } }
      expect(yield* SessionExecutionCapability.commitRenewal(sessionID, changed, "disclosure-renewal")).toBe(false)
      const renewed = { ...changed, revision: 5 }
      expect(yield* SessionExecutionCapability.commitRenewal(sessionID, renewed, "disclosure-renewal")).toBe(true)
      yield* SessionExecutionCapability.retain(sessionID, new Set())
      expect(yield* SessionExecutionCapability.get(sessionID, inputID)).toBeUndefined()
      expect(yield* SessionExecutionCapability.committedRenewal(sessionID, renewed, "disclosure-renewal")).toBe(true)
      expect(yield* SessionExecutionCapability.committedRenewal(sessionID, changed, "disclosure-renewal")).toBe(false)
      yield* SessionExecutionCapability.drop(sessionID, inputID)
      // drop intentionally clears a terminal input's witness as well as its grant.
      expect(yield* SessionExecutionCapability.committedRenewal(sessionID, renewed, "disclosure-renewal")).toBe(false)
      expect(yield* SessionExecutionCapability.set(sessionID, { ...renewed, revision: undefined })).toBe(false)
      yield* SessionExecutionCapability.clear(sessionID)
    }),
  )
})

it.effect("queued disclosure configuration cannot replace the captured active input", () =>
  Effect.gen(function* () {
    yield* SessionExecutionCapability.clear(sessionID)
    const config = {
      version: 1 as const,
      endpoint: "https://host.example.test/context",
      token: "actor-a",
      issuer: "host",
      audience: "native",
      keys: [{ id: "key", algorithm: "Ed25519" as const, publicKey: "a".repeat(43) }],
    }
    const queued = SessionMessage.ID.make("msg_queued_other_actor")
    yield* SessionExecutionCapability.set(sessionID, { inputID, mcp: {}, disclosure: config, revision: 1 })
    const captured = yield* SessionExecutionCapability.get(sessionID, inputID)
    yield* SessionExecutionCapability.set(sessionID, {
      inputID: queued,
      mcp: {},
      disclosure: { ...config, token: "actor-b" },
      revision: 1,
    })
    expect(SessionExecutionCapability.selectedInput(sessionID)).toBe(inputID)
    expect(captured?.disclosure?.token).toBe("actor-a")
    expect((yield* SessionExecutionCapability.get(sessionID, queued))?.disclosure?.token).toBe("actor-b")
    expect(captured?.disclosure?.token).toBe("actor-a")
    yield* SessionExecutionCapability.clear(sessionID)
  }),
)

it.effect("captures the exact selected installation without changing input selection or reviving replaced grants", () =>
  Effect.gen(function* () {
    const sessionID = Session.ID.create(),
      inputID = SessionMessage.ID.create(),
      queued = SessionMessage.ID.create()
    const service = {
      version: 1 as const,
      endpoint: "http://127.0.0.1:8123/preflight",
      token: "private-original",
      issuer: "fixture",
      audience: "fixture-context",
      keys: [{ id: "fixture", algorithm: "Ed25519" as const, publicKey: "a".repeat(43) }],
    }
    const grant = { inputID, disclosure: service, mcp: {}, revision: 7 }
    const missing = SessionExecutionCapability.captureSelection(sessionID, inputID, 7)
    expect(missing()).toBe(false)
    try {
      expect(yield* SessionExecutionCapability.set(sessionID, grant)).toBe(true)
      yield* SessionExecutionCapability.select(sessionID, inputID)
      const original = SessionExecutionCapability.captureSelection(sessionID, inputID, 7)
      expect(original()).toBe(true)
      expect(SessionExecutionCapability.captureSelection(sessionID, inputID, 8)()).toBe(false)
      expect(yield* SessionExecutionCapability.set(sessionID, structuredClone(grant))).toBe(true)
      expect(original()).toBe(true)
      yield* SessionExecutionCapability.set(sessionID, { ...grant, inputID: queued })
      yield* SessionExecutionCapability.select(sessionID, queued)
      expect(original()).toBe(false)
      expect(SessionExecutionCapability.selectedInput(sessionID)).toBe(queued)
      yield* SessionExecutionCapability.select(sessionID, inputID)
      expect(original()).toBe(true)
      yield* SessionExecutionCapability.clear(sessionID)
      yield* SessionExecutionCapability.set(sessionID, grant)
      yield* SessionExecutionCapability.select(sessionID, inputID)
      expect(original()).toBe(false)
      expect(missing()).toBe(false)
      const replaced = SessionExecutionCapability.captureSelection(sessionID, inputID, 7)
      expect(replaced()).toBe(true)
      expect(yield* SessionExecutionCapability.beginRenewal(sessionID, inputID, "replacement-claim")).toBeDefined()
      expect(replaced()).toBe(false)
      expect(
        yield* SessionExecutionCapability.commitRenewal(sessionID, { ...grant, revision: 8 }, "replacement-claim"),
      ).toBe(true)
      yield* SessionExecutionCapability.select(sessionID, inputID)
      expect(replaced()).toBe(false)
      expect(SessionExecutionCapability.captureSelection(sessionID, inputID, 8)()).toBe(true)
    } finally {
      yield* SessionExecutionCapability.clear(sessionID)
    }
  }),
)

describe("Exact publication requirements", () => {
  it.effect("requires the exact declared publication component and rejects unknown or future markers", () =>
    Effect.gen(function* () {
      for (let index = 0; index < 2; index++) {
        const session = Session.ID.create(),
          input = SessionMessage.ID.create()
        const grant = { inputID: input, mcp: {}, revision: 1 }
        expect(yield* SessionExecutionCapability.set(session, grant)).toBe(true)
        for (const capabilities of [
          ["publication-v1"],
          ["mcp", "disclosure", "publication-v1"],
          ["unrecognized"],
          ["mcp", "mcp"],
        ]) {
          expect(yield* SessionExecutionCapability.ready(session, input, capabilities)).toBe(false)
          expect(
            (yield* SessionRunnerLLM.executionGrant(session, [{ id: input, type: "user", capabilities }], grant).pipe(
              Effect.flip,
            ))._tag,
          ).toBe("Session.ExecutionCapabilityUnavailableError")
        }
        const disclosure = {
          version: 1 as const,
          publicationVersion: 1 as const,
          endpoint: "http://127.0.0.1/preflight",
          token: "private",
          issuer: "host",
          audience: "context",
          keys: [{ id: "key", algorithm: "Ed25519" as const, publicKey: "a".repeat(43) }],
        }
        expect(yield* SessionExecutionCapability.set(session, { ...grant, disclosure, revision: 2 })).toBe(true)
        expect(yield* SessionExecutionCapability.get(session, input)).toEqual({ ...grant, disclosure, revision: 2 })
        expect(yield* SessionExecutionCapability.ready(session, input, ["mcp", "disclosure", "publication-v1"])).toBe(
          true,
        )
        expect(yield* SessionExecutionCapability.ready(session, input, ["mcp", "disclosure", "publication-v2"])).toBe(
          false,
        )
        expect(
          (yield* SessionRunnerLLM.executionGrant(
            session,
            [{ id: input, type: "user", capabilities: ["mcp", "disclosure"] }],
            { ...grant, disclosure },
          ).pipe(Effect.flip))._tag,
        ).toBe("Session.ExecutionCapabilityUnavailableError")
        yield* SessionExecutionCapability.clear(session)
      }
    }),
  )
})
