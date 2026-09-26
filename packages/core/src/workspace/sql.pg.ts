import { Workspace } from "@opencode/schema/workspace"
import { bigint, pgTable, text } from "drizzle-orm/pg-core"
import type { WorkspaceDriver } from "./driver.js"

export const WorkspaceTable = pgTable("workspace", {
  id: text().$type<Workspace.ID>().primaryKey(),
  provider: text().notNull(),
  binding: text().$type<WorkspaceDriver.Binding>(),
  created_at: bigint({ mode: "number" }).notNull(),
  last_used_at: bigint({ mode: "number" }).notNull(),
})
