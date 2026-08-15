import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Database } from "@opencode-ai/core/database/database"
import * as SqliteProject from "@opencode-ai/core/project/sql"
import * as PostgresProject from "@opencode-ai/core/project/sql.pg"
import * as SqliteSession from "@opencode-ai/core/session/sql"
import * as PostgresSession from "@opencode-ai/core/session/sql.pg"
import * as SqliteEvent from "@opencode-ai/core/event/sql"
import * as PostgresEvent from "@opencode-ai/core/event/sql.pg"
import { eq, and, isNull } from "drizzle-orm"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"

const PG_URL = process.env.OPENCODE_DATABASE_URL ?? "postgres://postgres:devpass@localhost:5432/postgres"

interface EngineContext {
  name: "sqlite" | "postgres"
  layer: ReturnType<typeof Database.sqliteDatabaseLayer>
  ProjectTable: typeof SqliteProject.ProjectTable | typeof PostgresProject.ProjectTable
  SessionTable: typeof SqliteSession.SessionTable | typeof PostgresSession.SessionTable
  SessionMessageTable: typeof SqliteSession.SessionMessageTable | typeof PostgresSession.SessionMessageTable
  SessionInboxTable: typeof SqliteSession.SessionInboxTable | typeof PostgresSession.SessionInboxTable
  EventSequenceTable: typeof SqliteEvent.EventSequenceTable | typeof PostgresEvent.EventSequenceTable
  EventTable: typeof SqliteEvent.EventTable | typeof PostgresEvent.EventTable
}

const engines: EngineContext[] = [
  {
    name: "sqlite",
    layer: Database.sqliteDatabaseLayer(":memory:"),
    ProjectTable: SqliteProject.ProjectTable,
    SessionTable: SqliteSession.SessionTable,
    SessionMessageTable: SqliteSession.SessionMessageTable,
    SessionInboxTable: SqliteSession.SessionInboxTable,
    EventSequenceTable: SqliteEvent.EventSequenceTable,
    EventTable: SqliteEvent.EventTable,
  },
  {
    name: "postgres",
    layer: Database.postgresDatabaseLayer(PG_URL),
    ProjectTable: PostgresProject.ProjectTable,
    SessionTable: PostgresSession.SessionTable,
    SessionMessageTable: PostgresSession.SessionMessageTable,
    SessionInboxTable: PostgresSession.SessionInboxTable,
    EventSequenceTable: PostgresEvent.EventSequenceTable,
    EventTable: PostgresEvent.EventTable,
  },
]

describe("Dual-Engine Parity Matrix (SQLite vs PostgreSQL)", () => {
  for (const engine of engines) {
    describe(`Engine: ${engine.name.toUpperCase()}`, () => {
      test("1. Schema Bootstrap & Migration Idempotency", async () => {
        await Effect.runPromise(
          Effect.gen(function* () {
            const { db, config } = yield* Database.Service
            expect(config?.dialect).toBe(engine.name)

            // Applying migrations twice should be a clean, safe no-op
            yield* DatabaseMigration.apply(db, engine.name)
            yield* DatabaseMigration.apply(db, engine.name)

            const migrations = yield* db.all<{ id: string }>(
              engine.name === "postgres"
                ? sql`SELECT id FROM "migration"`
                : sql`SELECT id FROM migration`,
            )
            expect(migrations.length).toBeGreaterThanOrEqual(1)
          }).pipe(Effect.provide(engine.layer)),
        )
      })

      test("2. Project Lifecycle & JSON Serialization Parity", async () => {
        await Effect.runPromise(
          Effect.gen(function* () {
            const { db } = yield* Database.Service
            const prjID = `prj_${engine.name}_${Date.now()}`

            // Insert project with custom fields and array paths
            yield* db
              .insert(engine.ProjectTable)
              .values({
                id: prjID,
                worktree: "/tmp/parity-project",
                name: "Parity Test Project",
                icon_color: "#10b981",
                sandboxes: ["/tmp/sandbox-1", "/tmp/sandbox-2"],
              })
              .run()

            // Retrieve and inspect
            const project = yield* db
              .select()
              .from(engine.ProjectTable)
              .where(eq(engine.ProjectTable.id, prjID))
              .get()

            expect(project).toBeDefined()
            expect(project?.id).toBe(prjID)
            expect(project?.name).toBe("Parity Test Project")
            expect(project?.icon_color).toBe("#10b981")
            expect(Array.isArray(project?.sandboxes)).toBe(true)
            expect(project?.sandboxes).toEqual(["/tmp/sandbox-1", "/tmp/sandbox-2"])
            expect(typeof project?.time_created).toBe("number")
            expect(typeof project?.time_updated).toBe("number")

            // Clean up
            yield* db.delete(engine.ProjectTable).where(eq(engine.ProjectTable.id, prjID)).run()
          }).pipe(Effect.provide(engine.layer)),
        )
      })

      test("3. Session State, Cost, Token Accounting & Claims", async () => {
        await Effect.runPromise(
          Effect.gen(function* () {
            const { db } = yield* Database.Service
            const prjID = `prj_ses_${engine.name}_${Date.now()}`
            const sesID = `ses_${engine.name}_${Date.now()}`

            yield* db
              .insert(engine.ProjectTable)
              .values({ id: prjID, worktree: "/tmp/parity-ses", sandboxes: [] })
              .run()

            // Insert session
            yield* db
              .insert(engine.SessionTable)
              .values({
                id: sesID,
                project_id: prjID,
                slug: "parity-session",
                directory: "/tmp/parity-ses",
                version: "2.0",
                title: "Parity Session Title",
                cost: 0.045,
                tokens_input: 1200,
                tokens_output: 350,
                tokens_reasoning: 80,
                tokens_cache_read: 400,
                tokens_cache_write: 100,
              })
              .run()

            const session = yield* db
              .select()
              .from(engine.SessionTable)
              .where(eq(engine.SessionTable.id, sesID))
              .get()

            expect(session).toBeDefined()
            expect(session?.id).toBe(sesID)
            expect(session?.title).toBe("Parity Session Title")
            expect(session?.cost).toBeCloseTo(0.045, 3)
            expect(session?.tokens_input).toBe(1200)
            expect(session?.tokens_output).toBe(350)
            expect(session?.tokens_reasoning).toBe(80)
            expect(session?.tokens_cache_read).toBe(400)
            expect(session?.tokens_cache_write).toBe(100)

            // Update execution claim
            const claimTime = Date.now()
            yield* db
              .update(engine.SessionTable)
              .set({ time_suspended: claimTime })
              .where(eq(engine.SessionTable.id, sesID))
              .run()

            const claimed = yield* db
              .select()
              .from(engine.SessionTable)
              .where(and(eq(engine.SessionTable.id, sesID), isNull(engine.SessionTable.parent_id)))
              .get()

            expect(claimed?.time_suspended).toBe(claimTime)
            expect(typeof claimed?.time_suspended).toBe("number")

            // Release claim
            yield* db
              .update(engine.SessionTable)
              .set({ time_suspended: null })
              .where(eq(engine.SessionTable.id, sesID))
              .run()

            const released = yield* db
              .select()
              .from(engine.SessionTable)
              .where(eq(engine.SessionTable.id, sesID))
              .get()
            expect(released?.time_suspended).toBeNull()

            // Delete project (cascades to session)
            yield* db.delete(engine.ProjectTable).where(eq(engine.ProjectTable.id, prjID)).run()
            const orphaned = yield* db
              .select()
              .from(engine.SessionTable)
              .where(eq(engine.SessionTable.id, sesID))
              .get()
            expect(orphaned).toBeUndefined()
          }).pipe(Effect.provide(engine.layer)),
        )
      })

      test("4. Message Timeline & Inbox Queuing Parity", async () => {
        await Effect.runPromise(
          Effect.gen(function* () {
            const { db } = yield* Database.Service
            const prjID = `prj_tl_${engine.name}_${Date.now()}`
            const sesID = `ses_tl_${engine.name}_${Date.now()}`

            yield* db
              .insert(engine.ProjectTable)
              .values({ id: prjID, worktree: "/tmp/timeline", sandboxes: [] })
              .run()

            yield* db
              .insert(engine.SessionTable)
              .values({ id: sesID, project_id: prjID, slug: "timeline", directory: "/tmp/timeline", version: "2.0" })
              .run()

            // Insert 3 chronological messages
            for (let i = 1; i <= 3; i++) {
              yield* db
                .insert(engine.SessionMessageTable)
                .values({
                  id: `msg_${i}_${Date.now()}`,
                  session_id: sesID,
                  type: i === 1 ? "user" : "assistant",
                  seq: i,
                  data: JSON.stringify({
                    role: i === 1 ? "user" : "assistant",
                    parts: [{ type: "text", text: `Message step ${i}` }],
                  }),
                })
                .run()
            }

            const messages = yield* db
              .select()
              .from(engine.SessionMessageTable)
              .where(eq(engine.SessionMessageTable.session_id, sesID))
              .all()

            expect(messages.length).toBe(3)
            expect(messages[0].seq).toBe(1)
            expect(messages[1].seq).toBe(2)
            expect(messages[2].seq).toBe(3)
            expect(typeof messages[0].seq).toBe("number")

            // Test Inbox (steer vs queue delivery)
            const inboxSteerID = `inbox_steer_${Date.now()}`
            const inboxQueueID = `inbox_queue_${Date.now()}`

            yield* db
              .insert(engine.SessionInboxTable)
              .values([
                {
                  id: inboxSteerID,
                  session_id: sesID,
                  type: "user",
                  payload: JSON.stringify({ parts: [{ type: "text", text: "Interrupt steering" }] }),
                  delivery: "steer",
                  enqueued_seq: 4,
                },
                {
                  id: inboxQueueID,
                  session_id: sesID,
                  type: "user",
                  payload: JSON.stringify({ parts: [{ type: "text", text: "Sequential queuing" }] }),
                  delivery: "queue",
                  enqueued_seq: 5,
                },
              ])
              .run()

            const inboxItems = yield* db
              .select()
              .from(engine.SessionInboxTable)
              .where(eq(engine.SessionInboxTable.session_id, sesID))
              .all()

            expect(inboxItems.length).toBe(2)
            expect(inboxItems.find((item) => item.id === inboxSteerID)?.delivery).toBe("steer")
            expect(inboxItems.find((item) => item.id === inboxQueueID)?.delivery).toBe("queue")

            // Clean up
            yield* db.delete(engine.ProjectTable).where(eq(engine.ProjectTable.id, prjID)).run()
          }).pipe(Effect.provide(engine.layer)),
        )
      })

      test("5. Atomic Event Sourcing & Upsert Parity", async () => {
        await Effect.runPromise(
          Effect.gen(function* () {
            const { db } = yield* Database.Service
            const aggID = `agg_${engine.name}_${Date.now()}`

            // Insert initial sequence
            yield* db
              .insert(engine.EventSequenceTable)
              .values([{ aggregate_id: aggID, seq: 0, owner_id: "worker-1" }])
              .run()

            // Update onConflictDoUpdate
            yield* db
              .insert(engine.EventSequenceTable)
              .values([{ aggregate_id: aggID, seq: 1, owner_id: "worker-1" }])
              .onConflictDoUpdate({
                target: engine.EventSequenceTable.aggregate_id,
                set: { seq: 1 },
              })
              .run()

            const seqRow = yield* db
              .select()
              .from(engine.EventSequenceTable)
              .where(eq(engine.EventSequenceTable.aggregate_id, aggID))
              .get()

            expect(seqRow?.seq).toBe(1)
            expect(typeof seqRow?.seq).toBe("number")

            // Insert event
            yield* db
              .insert(engine.EventTable)
              .values({
                id: `evt_${Date.now()}`,
                aggregate_id: aggID,
                seq: 1,
                type: "session.turn.completed.1",
                data: JSON.stringify({ turns: 1, tokens: 250 }),
              })
              .run()

            const event = yield* db
              .select()
              .from(engine.EventTable)
              .where(eq(engine.EventTable.aggregate_id, aggID))
              .get()

            expect(event).toBeDefined()
            expect(event?.type).toBe("session.turn.completed.1")
            expect(typeof event?.seq).toBe("number")
            expect(event?.seq).toBe(1)

            // Clean up
            yield* db.delete(engine.EventSequenceTable).where(eq(engine.EventSequenceTable.aggregate_id, aggID)).run()
          }).pipe(Effect.provide(engine.layer)),
        )
      })

      test("6. Transaction Isolation & Rollback Parity", async () => {
        await Effect.runPromise(
          Effect.gen(function* () {
            const { db } = yield* Database.Service
            const prjID = `prj_tx_${engine.name}_${Date.now()}`

            // Insert outside transaction
            yield* db
              .insert(engine.ProjectTable)
              .values({ id: prjID, worktree: "/tmp/tx-parity", sandboxes: [] })
              .run()

            // Failing transaction must roll back
            const rollbackEffect = db.transaction((tx) =>
              Effect.gen(function* () {
                yield* tx.delete(engine.ProjectTable).where(eq(engine.ProjectTable.id, prjID)).run()
                yield* Effect.fail(new Error("deliberate-transaction-abort"))
              }),
            )

            const exit = yield* Effect.exit(rollbackEffect)
            expect(exit._tag).toBe("Failure")

            // Project must still exist due to rollback
            const preserved = yield* db
              .select()
              .from(engine.ProjectTable)
              .where(eq(engine.ProjectTable.id, prjID))
              .get()

            expect(preserved).toBeDefined()
            expect(preserved?.id).toBe(prjID)

            // Clean up
            yield* db.delete(engine.ProjectTable).where(eq(engine.ProjectTable.id, prjID)).run()
          }).pipe(Effect.provide(engine.layer)),
        )
      })
    })
  }
})
