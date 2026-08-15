import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import * as DatabaseConfig from "@opencode-ai/core/database/config"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { ProjectTable } from "@opencode-ai/core/project/sql.pg"
import { SessionTable, SessionMessageTable, SessionInboxTable } from "@opencode-ai/core/session/sql.pg"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql.pg"
import { CredentialTable } from "@opencode-ai/core/credential/sql.pg"
import { PermissionTable } from "@opencode-ai/core/permission/sql.pg"
import { KVTable } from "@opencode-ai/core/kv/sql.pg"
import { eq, and, isNull } from "drizzle-orm"

const PG_URL = process.env.OPENCODE_DATABASE_URL ?? "postgres://postgres:devpass@localhost:5432/postgres"

describe("PostgreSQL Backend Support", () => {
  const pgLayer = Database.postgresDatabaseLayer(PG_URL)

  test("bootstraps full PostgreSQL schema via DatabaseMigration.apply", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const { db, config } = yield* Database.Service
        expect(config?.dialect).toBe("postgres")

        // Verify tables exist in PostgreSQL information_schema
        const tables = yield* db.all<{ name: string }>(
          sql`SELECT table_name AS name FROM information_schema.tables WHERE table_schema = 'public'`,
        )
        const tableNames = new Set(tables.map((t) => t.name))

        expect(tableNames.has("project")).toBe(true)
        expect(tableNames.has("session_v2")).toBe(true)
        expect(tableNames.has("session_message")).toBe(true)
        expect(tableNames.has("session_inbox")).toBe(true)
        expect(tableNames.has("event_sequence")).toBe(true)
        expect(tableNames.has("event")).toBe(true)
        expect(tableNames.has("credential")).toBe(true)
        expect(tableNames.has("permission")).toBe(true)
        expect(tableNames.has("migration")).toBe(true)
      }).pipe(Effect.provide(pgLayer)),
    )
  })

  test("executes CRUD on Project and Session entities", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        const prjID = "prj_pg_test_" + Date.now()
        const sesID = "ses_pg_test_" + Date.now()

        // 1. Insert project
        yield* db
          .insert(ProjectTable)
          .values({
            id: prjID,
            worktree: "/tmp/pg-test",
            name: "PG Test Project",
            sandboxes: [],
          })
          .run()

        // 2. Query project
        const project = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, prjID)).get()
        expect(project).toBeDefined()
        expect(project?.id).toBe(prjID)
        expect(project?.name).toBe("PG Test Project")
        expect(Array.isArray(project?.sandboxes)).toBe(true)

        // 3. Insert session
        yield* db
          .insert(SessionTable)
          .values({
            id: sesID,
            project_id: prjID,
            slug: "pg-session-slug",
            directory: "/tmp/pg-test",
            version: "2.0",
            title: "Postgres Test Session",
          })
          .run()

        // 4. Query session
        const session = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sesID)).get()
        expect(session).toBeDefined()
        expect(session?.id).toBe(sesID)
        expect(session?.title).toBe("Postgres Test Session")
        expect(typeof session?.time_created).toBe("number")

        // 5. Update claim
        const now = Date.now()
        yield* db
          .update(SessionTable)
          .set({ time_suspended: now })
          .where(eq(SessionTable.id, sesID))
          .run()

        const updated = yield* db
          .select()
          .from(SessionTable)
          .where(and(eq(SessionTable.id, sesID), isNull(SessionTable.parent_id)))
          .get()
        expect(updated?.time_suspended).toBe(now)

        // 6. Delete project cascades to session
        yield* db.delete(ProjectTable).where(eq(ProjectTable.id, prjID)).run()
        const deletedSession = yield* db.select().from(SessionTable).where(eq(SessionTable.id, sesID)).get()
        expect(deletedSession).toBeUndefined()
      }).pipe(Effect.provide(pgLayer)),
    )
  })

  test("persists and sequences session messages and inbox items", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        const prjID = "prj_msg_" + Date.now()
        const sesID = "ses_msg_" + Date.now()
        const msgID = "msg_1_" + Date.now()
        const inboxID = "inbox_1_" + Date.now()

        yield* db.insert(ProjectTable).values({ id: prjID, worktree: "/tmp/msg", sandboxes: [] }).run()
        yield* db
          .insert(SessionTable)
          .values({ id: sesID, project_id: prjID, slug: "msg-test", directory: "/tmp/msg", version: "2.0" })
          .run()

        // Insert session message
        yield* db
          .insert(SessionMessageTable)
          .values({
            id: msgID,
            session_id: sesID,
            type: "user",
            seq: 1,
            data: JSON.stringify({ role: "user", text: "Hello Postgres" }),
          })
          .run()

        const messages = yield* db
          .select()
          .from(SessionMessageTable)
          .where(eq(SessionMessageTable.session_id, sesID))
          .all()
        expect(messages.length).toBe(1)
        expect(messages[0].id).toBe(msgID)
        expect(typeof messages[0].seq).toBe("number")
        expect(messages[0].seq).toBe(1)

        // Insert inbox item
        yield* db
          .insert(SessionInboxTable)
          .values({
            id: inboxID,
            session_id: sesID,
            type: "user",
            payload: JSON.stringify({ parts: [{ type: "text", text: "Queued prompt" }] }),
            delivery: "queue",
            enqueued_seq: 1,
          })
          .run()

        const inbox = yield* db
          .select()
          .from(SessionInboxTable)
          .where(eq(SessionInboxTable.session_id, sesID))
          .all()
        expect(inbox.length).toBe(1)
        expect(inbox[0].id).toBe(inboxID)
        expect(inbox[0].delivery).toBe("queue")

        // Cleanup
        yield* db.delete(ProjectTable).where(eq(ProjectTable.id, prjID)).run()
      }).pipe(Effect.provide(pgLayer)),
    )
  })

  test("manages event sequences and event rows atomically", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        const aggregateID = "agg_" + Date.now()
        const evt1 = "evt_1_" + Date.now()
        const evt2 = "evt_2_" + Date.now()

        // Initialize sequence
        yield* db
          .insert(EventSequenceTable)
          .values([{ aggregate_id: aggregateID, seq: 1 }])
          .onConflictDoUpdate({
            target: EventSequenceTable.aggregate_id,
            set: { seq: 1 },
          })
          .run()

        // Insert events
        yield* db
          .insert(EventTable)
          .values([
            { id: evt1, aggregate_id: aggregateID, seq: 0, type: "session.created.1", data: JSON.stringify({ a: 1 }) },
            { id: evt2, aggregate_id: aggregateID, seq: 1, type: "session.prompt.1", data: JSON.stringify({ b: 2 }) },
          ])
          .run()

        const events = yield* db
          .select()
          .from(EventTable)
          .where(eq(EventTable.aggregate_id, aggregateID))
          .all()
        expect(events.length).toBe(2)
        expect(typeof events[0].seq).toBe("number")
        expect(events[1].seq).toBe(1)

        // Cleanup
        yield* db.delete(EventSequenceTable).where(eq(EventSequenceTable.aggregate_id, aggregateID)).run()
      }).pipe(Effect.provide(pgLayer)),
    )
  })

  test("handles transactions and rollbacks", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        const prjID = "prj_tx_" + Date.now()

        // Successful transaction
        yield* db.transaction((tx) =>
          Effect.gen(function* () {
            yield* tx.insert(ProjectTable).values({ id: prjID, worktree: "/tmp/tx", sandboxes: [] }).run()
          }),
        )

        const created = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, prjID)).get()
        expect(created).toBeDefined()

        // Failing transaction rolls back
        const failTx = db.transaction((tx) =>
          Effect.gen(function* () {
            yield* tx.delete(ProjectTable).where(eq(ProjectTable.id, prjID)).run()
            yield* Effect.fail(new Error("rollback-intent"))
          }),
        )

        const result = yield* Effect.exit(failTx)
        expect(result._tag).toBe("Failure")

        // Record should still exist
        const stillExists = yield* db.select().from(ProjectTable).where(eq(ProjectTable.id, prjID)).get()
        expect(stillExists).toBeDefined()

        // Final cleanup
        yield* db.delete(ProjectTable).where(eq(ProjectTable.id, prjID)).run()
      }).pipe(Effect.provide(pgLayer)),
    )
  })
})
