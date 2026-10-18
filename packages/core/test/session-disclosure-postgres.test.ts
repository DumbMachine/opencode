import { expect, test } from "bun:test"
import { Context, Effect, Layer } from "effect"
import { eq } from "drizzle-orm"
import { Database } from "@opencode/core/database/database"
import { Global } from "@opencode/util/global"
import { AbsolutePath } from "@opencode/core/schema"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { SessionSchema } from "@opencode/core/session/schema"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionHistory } from "@opencode/core/session/history"
import { SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { Instructions } from "@opencode/core/instructions/index"
import { tmpdir } from "./fixture/tmpdir"

const url = process.env.OPENCODE_TEST_DATABASE_URL

test.skipIf(!url)(
  "retains disclosure requirements in actual PostgreSQL selected-history snapshots across original sessions",
  async () => {
    if (!new URL(url!).pathname.startsWith("/native_sharing_"))
      throw new Error("dedicated native_sharing_ database required")
    await using temporary = await tmpdir()
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const db = Context.get(yield* Layer.build(Database.postgresDatabaseLayer(url!)), Database.Service).db
          yield* db
            .insert(ProjectTable)
            .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
            .onConflictDoNothing()
            .run()
            .pipe(Effect.orDie)
          for (let index = 0; index < 2; index++) {
            const id = SessionSchema.ID.create()
            yield* db
              .insert(SessionTable)
              .values({ id, project_id: Project.ID.global, slug: id, directory: "/project", version: "test" })
              .run()
              .pipe(Effect.orDie)
            try {
              const ordinary = SessionMessage.ID.create()
              yield* db
                .insert(SessionMessageTable)
                .values({
                  id: ordinary,
                  session_id: id,
                  type: "user",
                  seq: 1,
                  data: { text: "Original human content", time: { created: 0 } },
                })
                .run()
                .pipe(Effect.orDie)
              expect(yield* SessionHistory.requiresDisclosure(db, id)).toBe(false)
              const initial = yield* SessionHistory.preview(db, id, Instructions.combine([]), "local")
              expect(initial.requiresDisclosure).toBe(false)
              const marked = SessionMessage.ID.create()
              yield* db
                .insert(SessionMessageTable)
                .values({
                  id: marked,
                  session_id: id,
                  type: "user",
                  seq: 2,
                  data: { text: "Protected original", capabilities: ["disclosure"], time: { created: 1 } },
                })
                .run()
                .pipe(Effect.orDie)
              const selected = yield* SessionHistory.preview(db, id, Instructions.combine([]), "local")
              expect(selected.requiresDisclosure).toBe(true)
              expect(selected.messages.some((message) => message.id === marked)).toBe(true)
              yield* db.delete(SessionMessageTable).where(eq(SessionMessageTable.id, marked)).run().pipe(Effect.orDie)
              expect(yield* SessionHistory.requiresDisclosure(db, id)).toBe(false)
              expect(selected.requiresDisclosure).toBe(true)
            } finally {
              yield* db.delete(SessionTable).where(eq(SessionTable.id, id)).run().pipe(Effect.orDie)
            }
          }
        }),
      ).pipe(Effect.provideService(Global.Service, Global.make({ data: temporary.path }))),
    )
  },
  15000,
)
