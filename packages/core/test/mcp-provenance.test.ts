import { expect, test } from "bun:test"
import { createPublicKey, generateKeyPairSync, sign, verify } from "node:crypto"
import { Server, WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/server"
import { Effect, Exit, Layer, Schema } from "effect"
import { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { ExecutionProducer } from "@opencode/schema/execution-producer"
import { SessionID } from "@opencode/schema/session-id"
import { SessionMessageID } from "@opencode/schema/session-message-id"
import { ConfigMCP } from "@opencode/schema/config/mcp"
import { NativeProducer } from "@opencode/core/execution-producer"
import { McpProvenance } from "@opencode/core/mcp/provenance"
import { McpClient } from "@opencode/core/mcp/client"
import { Hash } from "@opencode/util/hash"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Agent } from "@opencode/schema/agent"
import { Tool } from "@opencode/schema/tool"
import { Tool as ToolRegistry } from "@opencode/core/tool"
import { Image } from "@opencode/core/image"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { hostEnvironmentLayer } from "./fixture/environment"

const key = () => {
  const pair = generateKeyPairSync("ed25519")
  const publicKey = pair.publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("base64url")
  const descriptor = { id: Hash.sha256(publicKey), algorithm: "Ed25519" as const, publicKey }
  return {
    descriptor,
    signed: (payload: string) => ({
      keyID: descriptor.id,
      payload,
      signature: sign(null, Buffer.from(payload), pair.privateKey).toString("base64url"),
    }),
  }
}
const contextKey = key()
const resultKey = key()
const publicationKey = key()
const catalogKey = key()
const fixture = (actor = "first") => {
  const now = Date.now()
  const service = ExecutionDisclosure.Service.make({
    version: 1,
    endpoint: "http://127.0.0.1/context",
    token: "fixture-token",
    issuer: "fixture-host",
    audience: "model-context",
    keys: [contextKey.descriptor],
    catalog: { version: 1, audience: "catalog", keys: [catalogKey.descriptor] },
  })
  const context = ExecutionDisclosure.ContextProof.make({
    version: 1,
    issuer: service.issuer,
    audience: service.audience,
    sessionID: SessionID.make("ses_" + actor),
    inputID: SessionMessageID.make("msg_" + actor),
    kind: "primary",
    attempt: "attempt-" + actor,
    contextDigest: Hash.sha256("context-" + actor),
    componentsDigest: Hash.sha256("components-" + actor),
    contextReference: Hash.sha256("reference-" + actor),
    publication: {
      version: 1,
      endpoint: "http://127.0.0.1/publication",
      audience: "publication",
      keys: [publicationKey.descriptor],
      organizationID: "org-" + actor,
      threadID: "thread-" + actor,
      capabilityID: "capability-" + actor,
      capabilityGeneration: 1,
    },
    tools: { version: 1, audience: "tool-result", keys: [resultKey.descriptor] },
    mode: "complete",
    complete: true,
    issuedAt: now,
    expiresAt: now + 60_000,
  })
  const signed = contextKey.signed(JSON.stringify(context))
  const capture = McpProvenance.capture(service, signed)
  const invocation = ExecutionProducer.McpInvocation.make({
    version: 1,
    purpose: "native-mcp-invocation",
    contract: "mcp-invocation-v1",
    producerEpoch: "test-epoch",
    sessionID: context.sessionID,
    inputID: context.inputID,
    attempt: context.attempt,
    invocationID: "invocation-" + actor,
    messageID: SessionMessageID.make("msg_assistant_" + actor),
    toolCallID: "outer-" + actor,
    server: "server-first",
    tool: "echo",
    argumentsRevision: Hash.sha256("{}"),
    contextReference: context.contextReference,
    contextProofRevision: Hash.sha256(signed.payload),
    issuedAt: now,
  })
  const respond = (raw: string, call = invocation, change: Partial<ExecutionDisclosure.ToolResultProof> = {}) => {
    const now = Date.now()
    const proof = ExecutionDisclosure.ToolResultProof.make({
      version: 1,
      purpose: "native-mcp-result",
      issuer: service.issuer,
      audience: context.tools!.audience,
      organizationID: context.publication!.organizationID,
      threadID: context.publication!.threadID,
      capabilityID: context.publication!.capabilityID,
      capabilityGeneration: context.publication!.capabilityGeneration,
      sessionID: call.sessionID,
      inputID: call.inputID,
      contextReference: call.contextReference,
      contextProofRevision: call.contextProofRevision,
      attempt: call.attempt,
      invocationID: call.invocationID,
      messageID: call.messageID,
      toolCallID: call.toolCallID,
      server: call.server,
      tool: call.tool,
      argumentsRevision: call.argumentsRevision,
      responseRevision: Hash.sha256(raw),
      receiptReference: Hash.sha256("receipt-" + call.invocationID),
      authorizationVersion: "authorization-1",
      manifestBinding: Hash.sha256("manifest-" + actor),
      audienceBinding: Hash.sha256("audience-" + actor),
      mode: "complete",
      complete: true,
      issuedAt: now,
      expiresAt: now + 20_000,
      ...change,
    })
    // Publication's audience is intentionally distinct from the tool signer.
    const payload = JSON.stringify({ ...proof, audience: change.audience ?? context.tools!.audience })
    const snapshot = Buffer.from(raw).toString("base64")
    const body = JSON.parse(raw)
    return {
      ...body,
      _meta: {
        ...(body._meta ?? {}),
        [McpProvenance.ResultKey]: {
          version: 1,
          snapshot,
          proof: resultKey.signed(payload),
        },
      },
    }
  }
  return { service, context, signed, capture, invocation, respond }
}

test("compares all JSON fields, strict types and ordered arrays without key-order dependence", () => {
  const f = fixture()
  const raw =
    '{"content":[{"type":"text","text":"hello"},{"type":"image","data":"aGVsbG8=","mimeType":"image/png"}],"isError":true,"structuredContent":{"a":1,"nested":[true,null,"x"]},"custom":{"retain":true},"_meta":{"other":"metadata"}}'
  const actual = f.respond(raw)
  actual.structuredContent = { nested: [true, null, "x"], a: 1 }
  const verified = McpProvenance.result(f.capture, f.invocation, actual)
  expect(verified.result).toEqual(JSON.parse(raw))
  expect(verified.proof.responseRevision).toBe(Hash.sha256(raw))
  for (const changed of [
    { ...actual, custom: {} },
    { ...actual, isError: false },
    { ...actual, content: [...actual.content].reverse() },
    { ...actual, structuredContent: { a: "1", nested: [true, null, "x"] } },
    { ...actual, extra: "unsigned" },
  ])
    expect(() => McpProvenance.result(f.capture, f.invocation, changed)).toThrow()
})

test("removes only the carrier and distinguishes absent, empty and populated metadata", () => {
  const f = fixture()
  for (const raw of [
    '{"content":[]}',
    '{"content":[],"_meta":{}}',
    '{"content":[],"_meta":{"nested":{"retain":[]}}}',
    '{"content":[],"structuredContent":{"access.execution.result":"ordinary data"}}',
  ])
    expect(McpProvenance.result(f.capture, f.invocation, f.respond(raw)).result).toEqual(JSON.parse(raw))
  for (const raw of ['{"content":[],"_meta":null}', '{"content":[],"_meta":{"access.execution.result":{}}}'])
    expect(() => McpProvenance.result(f.capture, f.invocation, f.respond(raw))).toThrow()
})

test("rejects duplicate keys, rounded numbers, invalid JSON and non-JSON SDK values", () => {
  const f = fixture()
  for (const number of [
    "9007199254740993",
    "1000000000000000100",
    "9007199254740992",
    "0.10000000000000001",
    "1e400",
    "1e-400",
    "-0",
    "12345678901234567890",
  ]) {
    const raw = '{"content":[],"structuredContent":{"value":' + number + "}}"
    expect(() => McpProvenance.result(f.capture, f.invocation, f.respond(raw))).toThrow()
  }
  for (const number of ["9007199254740991", "0.1", "1e2", "1.00", "1e-7", "5e-324"]) {
    const raw = '{"content":[],"structuredContent":{"value":' + number + "}}"
    expect(McpProvenance.result(f.capture, f.invocation, f.respond(raw)).result).toEqual(JSON.parse(raw))
  }
  for (const raw of ['{"a":1,"a":1}', '{"a":{"b":1,"b":2}}', '{"a":1,}', '{/*x*/"a":1}', "{} {}", '{"x":"\\ud800"}'])
    expect(() => McpProvenance.parse(raw)).toThrow()
  for (const value of [
    { x: undefined },
    { x: NaN },
    { x: Infinity },
    { x: 1n },
    [, 1],
    new Date(),
    {
      get x() {
        throw new Error("getter ran")
      },
    },
  ])
    expect(() => McpProvenance.own(value)).toThrow()
  const dangerous = '{"__proto__":{"value":1},"constructor":2}'
  expect(Object.hasOwn(McpProvenance.parse(dangerous) as object, "__proto__")).toBeTrue()
})

test("rejects malformed, forged, expired and transplanted host results and context descriptors", () => {
  const f = fixture()
  const raw = '{"content":[]}'
  for (const change of [
    { audience: "publication" },
    { organizationID: "other" },
    { threadID: "other" },
    { capabilityID: "other" },
    { capabilityGeneration: 2 },
    { inputID: SessionMessageID.make("msg_other") },
    { invocationID: "other" },
    { server: "other" },
    { tool: "other" },
    { messageID: SessionMessageID.make("msg_other") },
    { toolCallID: "other" },
    { argumentsRevision: "a".repeat(64) },
    { contextProofRevision: "b".repeat(64) },
    { responseRevision: "c".repeat(64) },
    { complete: false },
    { expiresAt: Date.now() - 1 },
    { expiresAt: Date.now() + 60_000 },
  ])
    expect(() => McpProvenance.result(f.capture, f.invocation, f.respond(raw, f.invocation, change))).toThrow()
  const unknown = f.respond(raw, f.invocation, { mode: "actor-private-unknown", complete: false })
  expect(McpProvenance.result(f.capture, f.invocation, unknown).proof.complete).toBeFalse()
  const valid = f.respond(raw)
  const carrier = valid._meta[McpProvenance.ResultKey]
  for (const altered of [
    { ...valid, _meta: {} },
    { ...valid, _meta: { [McpProvenance.ResultKey]: { ...carrier, extra: true } } },
    { ...valid, _meta: { [McpProvenance.ResultKey]: { ...carrier, snapshot: carrier.snapshot + "\n" } } },
    {
      ...valid,
      _meta: { [McpProvenance.ResultKey]: { ...carrier, proof: { ...carrier.proof, signature: "a".repeat(86) } } },
    },
  ])
    expect(() => McpProvenance.result(f.capture, f.invocation, altered)).toThrow()
  expect(() => McpProvenance.result(fixture("second").capture, f.invocation, valid)).toThrow()
  for (const change of [
    { tools: undefined },
    { publication: undefined },
    { audience: "other" },
    { expiresAt: Date.now() - 1 },
    { tools: { ...f.context.tools!, keys: [contextKey.descriptor] } },
    { tools: { ...f.context.tools!, audience: f.service.audience } },
    { tools: { ...f.context.tools!, keys: [resultKey.descriptor, resultKey.descriptor] } },
  ])
    expect(() =>
      McpProvenance.capture(f.service, contextKey.signed(JSON.stringify({ ...f.context, ...change }))),
    ).toThrow()
  expect(() => McpProvenance.capture(f.service, { ...f.signed, signature: "a".repeat(86) })).toThrow()
  const duplicate = f.signed.payload.replace("{", '{"version":1,')
  expect(() => McpProvenance.capture(f.service, contextKey.signed(duplicate))).toThrow()
})

const server = (name: string, actors: ReturnType<typeof fixture>[], enrollment: ExecutionProducer.Enrollment) =>
  Effect.acquireRelease(
    Effect.promise(async () => {
      const calls: ExecutionProducer.McpInvocation[] = []
      const protocol = new Server({ name, version: "1" }, { capabilities: { tools: {} } })
      protocol.setRequestHandler("tools/call", async (request) => {
        const carrier = request.params._meta?.[McpProvenance.RequestKey] as
          | {
              context: ExecutionDisclosure.SignedProof
              invocation: ExecutionProducer.SignedFact
            }
          | undefined
        if (!carrier) return { content: [{ type: "text", text: "ordinary" }] }
        const actor = actors.find((actor) => actor.signed.payload === carrier.context.payload)
        if (!actor) throw new Error("Wrong actor")
        const call = Schema.decodeUnknownSync(ExecutionProducer.McpInvocation)(JSON.parse(carrier.invocation.payload))
        expect(
          verify(
            null,
            Buffer.from(ExecutionProducer.SignatureDomain + carrier.invocation.payload),
            createPublicKey({
              key: Buffer.concat([
                Buffer.from("302a300506032b6570032100", "hex"),
                Buffer.from(enrollment.key.publicKey, "base64url"),
              ]),
              format: "der",
              type: "spki",
            }),
            Buffer.from(carrier.invocation.signature, "base64url"),
          ),
        ).toBeTrue()
        expect(call.server).toBe(name)
        expect(call.sessionID).toBe(actor.context.sessionID)
        expect(call.argumentsRevision).toBe(Hash.sha256(JSON.stringify(request.params.arguments)))
        calls.push(call)
        if (request.params.arguments?.rounded)
          return actor.respond('{"content":[],"structuredContent":{"number":1000000000000000100}}', call)
        await new Promise((resolve) => setTimeout(resolve, request.params.arguments?.index === 0 ? 20 : 1))
        return actor.respond(
          JSON.stringify({
            content: [
              { type: "text", text: actor.context.inputID },
              { type: "resource_link", uri: "docs://" + name, name: "reference" },
            ],
            structuredContent: { server: name, actor: actor.context.inputID },
            _meta: { retain: name },
          }),
          call,
        )
      })
      const transport = new WebStandardStreamableHTTPServerTransport({
        sessionIdGenerator: () => crypto.randomUUID(),
        enableJsonResponse: true,
      })
      await protocol.connect(transport)
      const http = Bun.serve({ port: 0, fetch: (request) => transport.handleRequest(request) })
      return {
        calls,
        url: http.url.toString(),
        close: async () => {
          await http.stop(true)
          await protocol.close()
        },
      }
    }),
    (server) => Effect.promise(server.close),
  )

test("actual SDK transport isolates two servers and actors and collects every concurrent nested response before conversion", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const actors = [fixture("first"), fixture("second")]
        const enrollment = yield* NativeProducer.enroll({ version: 1, challenge: "c".repeat(43), audience: "fixture" })
        const servers = yield* Effect.all([
          server("server-first", actors, enrollment),
          server("server-second", actors, enrollment),
        ])
        const connections = yield* Effect.all(
          servers.map((server, index) =>
            McpClient.connect(
              index === 0 ? "server-first" : "server-second",
              new ConfigMCP.Remote({ type: "remote", url: server.url, oauth: false }),
              import.meta.dir,
            ).pipe(Effect.provide(hostEnvironmentLayer)),
          ),
        )
        const outputs = yield* Effect.all(
          actors.map((actor) =>
            Effect.gen(function* () {
              const scope = yield* NativeProducer.captureTools({
                service: actor.service,
                signed: actor.signed,
                current: () => true,
              })
              return yield* scope.run(
                { messageID: actor.invocation.messageID, toolCallID: actor.invocation.toolCallID },
                Effect.all(
                  connections.flatMap((connection) =>
                    [0, 1].map((index) =>
                      connection.callTool({
                        name: "echo",
                        args: { index, nested: { key: "value" } },
                        sessionID: actor.context.sessionID,
                      }),
                    ),
                  ),
                  { concurrency: "unbounded" },
                ),
              )
            }),
          ),
          { concurrency: "unbounded" },
        )
        expect(servers.flatMap((server) => server.calls)).toHaveLength(8)
        expect(new Set(servers.flatMap((server) => server.calls.map((call) => call.invocationID))).size).toBe(8)
        for (const [index, output] of outputs.entries()) {
          expect(output.observations).toHaveLength(4)
          for (const observation of output.observations) {
            expect(observation.proof.inputID).toBe(actors[index].context.inputID)
            expect(observation.proof.toolCallID).toBe(actors[index].invocation.toolCallID)
            expect(observation.snapshot).toContain('"type":"resource_link"')
          }
          for (const result of output.value) {
            expect(result.meta).not.toHaveProperty(McpProvenance.ResultKey)
            expect(result.content[1].type).toBe("text")
          }
        }
        const numericScope = yield* NativeProducer.captureTools({
          service: actors[0].service,
          signed: actors[0].signed,
          current: () => true,
        })
        const rounded = yield* Effect.exit(
          numericScope.run(
            {
              messageID: actors[0].invocation.messageID,
              toolCallID: "rounded-result",
            },
            connections[0].callTool({
              name: "echo",
              args: { rounded: true },
              sessionID: actors[0].context.sessionID,
            }),
          ),
        )
        expect(Exit.isFailure(rounded)).toBeTrue()
        const ordinary = yield* connections[0].callTool({ name: "echo" })
        expect(ordinary.content).toEqual([{ type: "text", text: "ordinary" }])
      }),
    ),
  )
})

test("rejects scope substitution, reserved collisions, cancellation and mutable arguments before dispatch", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const f = fixture()
      let active = true
      let dispatched = 0
      const scope = yield* NativeProducer.captureTools({ service: f.service, signed: f.signed, current: () => active })
      const identity = { messageID: f.invocation.messageID, toolCallID: f.invocation.toolCallID }
      const transport = () =>
        Effect.sync(() => {
          dispatched++
          return {}
        })
      for (const candidate of [
        { sessionID: "ses_wrong", name: "echo" },
        { sessionID: f.context.sessionID, name: "echo", meta: { [McpProvenance.RequestKey]: {} } },
        { sessionID: f.context.sessionID, name: "echo", args: { value: undefined } },
        {
          sessionID: f.context.sessionID,
          name: "echo",
          args: {
            get value() {
              throw new Error("getter ran")
            },
          },
        },
      ]) {
        const exit = yield* Effect.exit(
          scope.run(identity, NativeProducer.dispatchMcp({ server: "server-first", ...candidate }, transport)),
        )
        expect(Exit.isFailure(exit)).toBeTrue()
      }
      active = false
      expect(
        Exit.isFailure(
          yield* Effect.exit(
            scope.run(
              identity,
              NativeProducer.dispatchMcp(
                {
                  server: "server-first",
                  name: "echo",
                  sessionID: f.context.sessionID,
                },
                transport,
              ),
            ),
          ),
        ),
      ).toBeTrue()
      expect(dispatched).toBe(0)
    }),
  )
})

test("verifies raw initialize and paginated catalog responses before SDK conversion and preserves later tool calls", async () => {
  const f = fixture()
  const envelope = (actual: object, kind: "initialize" | "tools/list", cursor: string) => {
    const raw = JSON.stringify(actual)
    const now = Date.now()
    const identity = f.context.publication!
    const proof = ExecutionDisclosure.CatalogProof.make({
      version: 1,
      purpose: "native-mcp-catalog",
      issuer: f.service.issuer,
      audience: f.service.catalog!.audience,
      organizationID: identity.organizationID,
      threadID: identity.threadID,
      sessionID: f.context.sessionID,
      inputID: f.context.inputID,
      capabilityID: identity.capabilityID,
      capabilityGeneration: 1,
      kind,
      cursor,
      responseRevision: Hash.sha256(raw),
      receiptReference: Hash.sha256(kind + cursor),
      authorizationVersion: "catalog-authorization",
      manifestBinding: Hash.sha256("catalog-manifest"),
      audienceBinding: Hash.sha256("catalog-audience"),
      mode: "complete",
      complete: true,
      issuedAt: now,
      expiresAt: now + 20_000,
    })
    return {
      ...actual,
      _meta: {
        [McpProvenance.CatalogKey]: {
          version: 1,
          snapshot: Buffer.from(raw).toString("base64"),
          proof: catalogKey.signed(JSON.stringify(proof)),
        },
      },
    }
  }
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const local = yield* Effect.acquireRelease(
          Effect.promise(async () => {
            const protocol = new Server(
              { name: "catalog", version: "1" },
              {
                capabilities: { tools: {} },
                instructions: "  Preserve these instructions.\n",
              },
            )
            protocol.setRequestHandler("tools/list", ({ params }) =>
              Promise.resolve({
                tools: [{ name: params?.cursor ? "second" : "first", inputSchema: { type: "object" as const } }],
                ...(params?.cursor ? {} : { nextCursor: "second-page" }),
              }),
            )
            protocol.setRequestHandler("tools/call", () =>
              Promise.resolve({ content: [{ type: "text", text: "ordinary tool still works" }] }),
            )
            const transport = new WebStandardStreamableHTTPServerTransport({
              sessionIdGenerator: () => crypto.randomUUID(),
              enableJsonResponse: true,
            })
            await protocol.connect(transport)
            const http = Bun.serve({
              port: 0,
              fetch: async (request) => {
                const call =
                  request.method === "POST"
                    ? ((await request.clone().json()) as { method?: string; params?: { cursor?: string } })
                    : undefined
                const response = await transport.handleRequest(request)
                if (call?.method !== "initialize" && call?.method !== "tools/list") return response
                const packet = (await response.json()) as { result?: Record<string, unknown> }
                if (!packet.result) return Response.json(packet, { status: response.status, headers: response.headers })
                return Response.json(
                  {
                    ...packet,
                    result: envelope(
                      packet.result,
                      call.method as "initialize" | "tools/list",
                      call.params?.cursor ?? "",
                    ),
                  },
                  { status: response.status, headers: response.headers },
                )
              },
            })
            return {
              url: http.url.toString(),
              close: async () => {
                await http.stop(true)
                await protocol.close()
              },
            }
          }),
          (server) => Effect.promise(server.close),
        )
        const captured = yield* NativeProducer.captureCatalog({
          service: f.service,
          sessionID: f.context.sessionID,
          inputID: f.context.inputID,
          revision: 1,
          current: () => true,
        }).run(
          Effect.gen(function* () {
            const connection = yield* McpClient.connect(
              "catalog",
              new ConfigMCP.Remote({ type: "remote", url: local.url, oauth: false }),
              import.meta.dir,
            ).pipe(Effect.provide(hostEnvironmentLayer))
            expect(connection.instructions).toBe("  Preserve these instructions.\n")
            expect((yield* connection.tools()).map((tool) => tool.name)).toEqual(["first", "second"])
            return connection
          }),
        )
        expect(captured.evidence.complete).toBeTrue()
        expect(captured.evidence.observations.map((item) => [item.proof.kind, item.proof.cursor])).toEqual([
          ["initialize", ""],
          ["tools/list", ""],
          ["tools/list", "second-page"],
        ])
        expect((yield* captured.value.callTool({ name: "first" })).content).toEqual([
          { type: "text", text: "ordinary tool still works" },
        ])
        const observation = captured.evidence.observations[0]
        expect(() =>
          McpProvenance.catalog(
            {
              service: f.service,
              sessionID: f.context.sessionID,
              inputID: f.context.inputID,
              revision: 2,
              server: "catalog",
              kind: "initialize",
              cursor: "",
            },
            envelope(JSON.parse(observation.snapshot), "initialize", ""),
          ),
        ).toThrow()
      }),
    ),
  )
})

test("retains typed transport failure evidence as incomplete and refuses unbound actor tools", async () => {
  const f = fixture()
  const capture = await Effect.runPromise(
    NativeProducer.captureTools({ service: f.service, signed: f.signed, current: () => true }),
  )
  const failure = new Error("unverified remote private failure")
  const identity = { messageID: f.invocation.messageID, toolCallID: "transport-error" }
  const result = await Effect.runPromise(
    capture
      .run(
        identity,
        NativeProducer.dispatchMcp(
          { server: "server-first", name: "echo", args: {}, sessionID: f.context.sessionID },
          () => Effect.fail(failure),
        ),
      )
      .pipe(Effect.exit),
  )
  expect(Exit.isFailure(result)).toBeTrue()
  const fact = NativeProducer.projectToolUnit(Hash.sha256("terminal"), failure)
  expect(fact).toBeDefined()
  expect(JSON.parse(fact!.payload)).toMatchObject({ complete: false, parentReceipts: [] })
  const denied = await Effect.runPromise(capture.run(identity, NativeProducer.requireToolBinding).pipe(Effect.exit))
  expect(Exit.isFailure(denied)).toBeTrue()
  await Effect.runPromise(NativeProducer.requireToolBinding)
})

test("refuses unbound CodeMode source access and plugin callbacks before invocation while ordinary hooks remain compatible", async () => {
  const f = fixture()
  const capture = await Effect.runPromise(
    NativeProducer.captureTools({ service: f.service, signed: f.signed, current: () => true }),
  )
  const identity = { messageID: f.invocation.messageID, toolCallID: "hook-gate" }
  let invoked = 0
  await Effect.runPromise(
    Effect.gen(function* () {
      const hooks = yield* PluginHooks.Service
      yield* hooks.register("tool", "execute.before", () =>
        Effect.sync(() => {
          invoked++
        }),
      )
      const event = {
        tool: "example",
        sessionID: f.context.sessionID,
        agent: Agent.ID.make("fixture"),
        messageID: identity.messageID,
        id: Tool.CallID.make(identity.toolCallID),
        input: {},
      }
      expect(
        Exit.isFailure(
          yield* capture.run(identity, hooks.triggerObserved("tool", "execute.before", event)).pipe(Effect.exit),
        ),
      ).toBeTrue()
      expect(invoked).toBe(0)
      yield* hooks.triggerObserved("tool", "execute.before", event)
      expect(invoked).toBe(1)
      let resolved = 0
      yield* hooks.register("tool", "resolve", () =>
        Effect.sync(() => {
          resolved++
        }),
      )
      const resolve = { sessionID: f.context.sessionID, agent: Agent.ID.make("fixture"), tools: [], renames: [] }
      expect(
        Exit.isFailure(
          yield* NativeProducer.managedPreparation(hooks.trigger("tool", "resolve", resolve)).pipe(Effect.exit),
        ),
      ).toBeTrue()
      expect(resolved).toBe(0)
      yield* hooks.trigger("tool", "resolve", resolve)
      expect(resolved).toBe(1)
      let physical = 0
      expect(
        Exit.isFailure(
          yield* capture
            .run(
              identity,
              NativeProducer.unknownToolSource.pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    physical++
                  }),
                ),
              ),
            )
            .pipe(Effect.exit),
        ),
      ).toBeTrue()
      expect(physical).toBe(0)
    }).pipe(Effect.scoped, Effect.provide(AppNodeBuilder.build(PluginHooks.node))),
  )
})


test("the canonical executor denies unbound native leaves inside CodeMode before execution", async () => {
  const f = fixture()
  let physical = 0
  await Effect.runPromise(Effect.gen(function* () {
    const tools = yield* ToolRegistry.Service
    yield* tools.transform(editor => editor.add({ name: "ambient", description: "Unsupported actor binding", input: Schema.Struct({}),
      execute: () => Effect.sync(() => { physical++; return { content: "ambient bytes" } }) }))
    const snapshot = yield* tools.snapshot()
    const capture = yield* NativeProducer.captureTools({ service: f.service, signed: f.signed, current: () => true })
    const identity = { messageID: f.invocation.messageID, toolCallID: "nested-native" }
    const request = { sessionID: f.context.sessionID, inputID: f.context.inputID, agent: Agent.ID.make("fixture"), messageID: identity.messageID,
      call: { type: "tool-call" as const, id: identity.toolCallID, name: "execute", input: { code: "return await tools.ambient({})" } } }
    const result = yield* capture.run(identity, snapshot.execute(request)).pipe(Effect.exit)
    expect(physical).toBe(0)
    expect(Exit.isFailure(result) || (Exit.isSuccess(result) && JSON.stringify(result.value).includes("Access needed"))).toBeTrue()
    yield* snapshot.execute({ ...request, call: { ...request.call, id: "ordinary", input: { code: "return await tools.ambient({})" } } })
    expect(physical).toBe(1)
  }).pipe(Effect.scoped, Effect.provide(AppNodeBuilder.build(LayerNode.group([ToolRegistry.node, PluginHooks.node]), [
    Image.node.replace(Layer.mock(Image.Service, { normalize: () => Effect.die("unused image normalization") })),
  ]))))
})
