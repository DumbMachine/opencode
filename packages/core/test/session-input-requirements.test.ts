import { expect } from "bun:test"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Database } from "@opencode/core/database/database"
import { SessionHistory } from "@opencode/core/session/history"
import { SessionTable } from "@opencode/core/session/sql"
import { ProjectTable } from "@opencode/core/project/sql"
import { Project } from "@opencode/schema/project"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { AbsolutePath } from "@opencode/schema/schema"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(Database.node))
it.effect("retains SQLite requirement node types and blocks malformed hidden history", () =>
  Effect.gen(function* () {
    const database = yield* Database.Service
    yield* database.db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .run()
      .pipe(Effect.orDie)
    for (let index = 0; index < 2; index++) {
      const id = Session.ID.create(),
        message = SessionMessage.ID.create()
      yield* database.db
        .insert(SessionTable)
        .values({ id, project_id: Project.ID.global, slug: id, directory: "/project", version: "test" })
        .run()
        .pipe(Effect.orDie)
      yield* database.db
        .run(
          sql`INSERT INTO session_message (id,session_id,type,seq,data,time_created,time_updated) VALUES (${message},${id},'user',0,'{}',0,0)`,
        )
        .pipe(Effect.orDie)
      for (const [requirement, blocked] of [
        [undefined, false],
        [[], false],
        [["mcp"], false],
        [["disclosure"], true],
        [["publication-v1"], true],
        ['["mcp"]', true],
        [null, true],
        [{}, true],
        [true, true],
        [false, true],
        [42, true],
        [1.5, true],
        [["mcp", 1], true],
      ] as const) {
        const data = JSON.stringify({
          text: "Retained requirement",
          ...(requirement === undefined ? {} : { capabilities: requirement }),
          time: { created: 1 },
        })
        yield* database.db.run(sql`UPDATE session_message SET data=${data} WHERE id=${message}`).pipe(Effect.orDie)
        expect(yield* SessionHistory.hasUnsupportedInput(database.db, id)).toBe(blocked)
      }
    }
  }),
)
