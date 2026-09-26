import { isNotNull, isNull, ne, or } from "drizzle-orm"
import { pgTable, text, primaryKey, bigint } from "drizzle-orm/pg-core"
import { absoluteArrayColumn, absoluteColumn } from "../database/path.pg.js"
import { Timestamps } from "../database/schema.sql.pg.js"
import type { AbsolutePath } from "../schema.js"
import { ProjectSchema } from "./schema.js"

export const ProjectTable = pgTable("project", {
  id: text().$type<ProjectSchema.ID>().primaryKey(),
  worktree: absoluteColumn().notNull(),
  vcs: text().$type<"git" | "hg">(),
  name: text(),
  icon_url: text(),
  icon_url_override: text(),
  icon_color: text(),
  ...Timestamps,
  time_initialized: bigint({ mode: "number" }),
  time_active: bigint({ mode: "number" })
    .notNull()
    .default(0)
    .$defaultFn(() => Date.now()),
  sandboxes: absoluteArrayColumn().notNull(),
  commands: text().$type<{ start?: string }>(),
})

/** @deprecated Use WorktreeTable from worktree/sql instead. */
export const ProjectDirectoryTable = pgTable(
  "project_directory",
  {
    project_id: text()
      .$type<ProjectSchema.ID>()
      .notNull()
      .references(() => ProjectTable.id, { onDelete: "cascade" }),
    directory: absoluteColumn().notNull(),
    type: text().$type<"main" | "root" | "git_worktree">(),
    strategy: text(),
    time_created: bigint({ mode: "number" })
      .notNull()
      .$default(() => Date.now()),
  },
  (table) => [primaryKey({ columns: [table.project_id, table.directory] })],
)

export function upsertProject(
  db: any,
  project: { readonly id: ProjectSchema.ID; readonly canonical: AbsolutePath; readonly vcs?: ProjectSchema.Vcs },
) {
  const vcs = project.vcs?.type
  return db
    .insert(ProjectTable)
    .values({ id: project.id, worktree: project.canonical, vcs, sandboxes: [] })
    .onConflictDoUpdate({
      target: ProjectTable.id,
      set: { worktree: project.canonical, vcs: vcs ?? null },
      setWhere: or(
        ne(ProjectTable.worktree, project.canonical),
        vcs
          ? or(isNull(ProjectTable.vcs as any), ne(ProjectTable.vcs as any, vcs))
          : isNotNull(ProjectTable.vcs as any),
      ) as any,
    })
    .run()
}
