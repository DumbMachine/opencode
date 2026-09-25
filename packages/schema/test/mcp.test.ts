import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { Mcp } from "../src/mcp.js"

describe("Mcp resources", () => {
  test("decodes resource catalogs and omits absent metadata", () => {
    const value = Schema.decodeUnknownSync(Mcp.ResourceCatalog)({
      resources: [{ server: "docs", name: "Readme", uri: "docs://readme" }],
      templates: [{ server: "docs", name: "File", uriTemplate: "docs://{path}" }],
    })

    expect(Schema.encodeSync(Mcp.ResourceCatalog)(value)).toEqual({
      resources: [{ server: "docs", name: "Readme", uri: "docs://readme" }],
      templates: [{ server: "docs", name: "File", uriTemplate: "docs://{path}" }],
    })
  })

  test("preserves text and base64 blob contents", () => {
    expect(
      Schema.decodeUnknownSync(Mcp.ResourceContent)({
        server: "docs",
        uri: "docs://readme",
        contents: [
          { type: "text", uri: "docs://readme", text: "hello", mimeType: "text/plain" },
          { type: "blob", uri: "docs://logo", blob: "aGVsbG8=", mimeType: "image/png" },
        ],
      }),
    ).toEqual({
      server: "docs",
      uri: "docs://readme",
      contents: [
        { type: "text", uri: "docs://readme", text: "hello", mimeType: "text/plain" },
        { type: "blob", uri: "docs://logo", blob: "aGVsbG8=", mimeType: "image/png" },
      ],
    })
  })
})

describe("request-scoped MCP", () => {
  test("accepts remote servers with caller-supplied headers", () => {
    expect(
      Schema.decodeUnknownSync(Mcp.RequestServers)({
        tenant: { type: "remote", url: "https://mcp.example.test", headers: { authorization: "Bearer short" } },
      }),
    ).toEqual({
      tenant: { type: "remote", url: "https://mcp.example.test", headers: { authorization: "Bearer short" } },
    })
  })

  test("preserves request-only approval bridge selection", () => {
    const decoded = Schema.decodeUnknownSync(Mcp.RequestServers)({
      tenant: { type: "remote", url: "https://mcp.example.test", approval_bridge: ["*"] },
    })
    expect(decoded.tenant?.approval_bridge).toEqual(["*"])
  })

  test("rejects local processes and persisted OAuth configuration", () => {
    expect(() =>
      Schema.decodeUnknownSync(Mcp.RequestServers)({ local: { type: "local", command: ["server"] } }),
    ).toThrow()
    expect(() =>
      Schema.decodeUnknownSync(Mcp.RequestServers)({
        tenant: { type: "remote", url: "https://mcp.example.test", oauth: { client_id: "persisted" } },
      }),
    ).toThrow()
  })
})
