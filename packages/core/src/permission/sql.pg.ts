import { pgTable, text, uniqueIndex } from "drizzle-orm/pg-core"
import { Timestamps } from "../database/schema.sql.pg.js"
import { Project } from "../project.js"
import { ProjectTable } from "../project/sql.pg.js"
import type { PermissionSaved } from "./saved.js"

export const PermissionTable = pgTable(
  "permission",
  {
    id: text().$type<PermissionSaved.ID>().primaryKey(),
    project_id: text()
      .$type<Project.ID>()
      .notNull()
      .references(() => ProjectTable.id, { onDelete: "cascade" }),
    action: text().notNull(),
    resource: text().notNull(),
    ...Timestamps,
  },
  (table) => [uniqueIndex("permission_project_action_resource_idx").on(table.project_id, table.action, table.resource)],
)
