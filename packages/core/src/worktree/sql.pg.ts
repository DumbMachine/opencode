import { bigint, primaryKey, pgTable, text } from "drizzle-orm/pg-core"
import { absoluteColumn } from "../database/path.pg.js"
import { ProjectSchema } from "../project/schema.js"
import { ProjectTable } from "../project/sql.pg.js"

export const WorktreeTable = pgTable(
  "worktree",
  {
    project_id: text()
      .$type<ProjectSchema.ID>()
      .notNull()
      .references(() => ProjectTable.id, { onDelete: "cascade" }),
    directory: absoluteColumn().notNull(),
    strategy: text(),
    time_created: bigint({ mode: "number" })
      .notNull()
      .$default(() => Date.now()),
  },
  (table) => [primaryKey({ columns: [table.project_id, table.directory] })],
)
