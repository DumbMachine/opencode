import { expect } from "bun:test"
import { Effect } from "effect"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

it.live("public request creation cannot assert native input provenance", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make({
      app: { version: "test" },
      database: { path: ":memory:" },
      fs: { filewatcher: false },
      models: { fetch: false },
    })
    const request = (path: string, method: string, body?: unknown) =>
      Effect.promise(async () => {
        const response = await handler(
          new Request(`http://opencode.local${path}`, {
            method,
            headers: { "content-type": "application/json" },
            body: body === undefined ? undefined : JSON.stringify(body),
          }),
        )
        expect(response.status).toBe(200)
        return response.json()
      })
    const created = yield* request("/api/session", "POST", {
      permissions: [{ action: "unassigned_action", resource: "*", effect: "ask" }],
    })
    const sessionID = created.data.id
    const source = { type: "tool", messageID: "msg_assistant", id: "call_claimed", inputID: "msg_foreign_input" }
    yield* request(`/api/session/${sessionID}/permission`, "POST", {
      id: "per_forged_source",
      action: "unassigned_action",
      resources: ["*"],
      source,
    })
    const permission = yield* request(`/api/session/${sessionID}/permission/per_forged_source`, "GET")
    expect(permission.data.source).toEqual({ type: "tool", messageID: "msg_assistant", id: "call_claimed" })
    const form = yield* request(`/api/session/${sessionID}/form`, "POST", {
      id: "frm_forged_source",
      title: "Untrusted form",
      source,
      metadata: { kind: "question", tool: source },
      fields: [{ key: "answer", type: "string" }],
    })
    expect(form.data.source).toBeUndefined()
    expect(form.data.metadata.tool.inputID).toBe("msg_foreign_input")
    const listed = yield* request(`/api/session/${sessionID}/form`, "GET")
    expect(listed.data[0]?.source).toBeUndefined()
  }).pipe(Effect.scoped),
)
