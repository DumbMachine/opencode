import { pgTable, text, integer, primaryKey } from "drizzle-orm/pg-core"
import { Timestamps } from "../database/schema.sql.pg.js"

export const AccountTable = pgTable("account", {
  id: text().primaryKey(),
  email: text().notNull(),
  url: text().notNull(),
  access_token: text().notNull(),
  refresh_token: text().notNull(),
  token_expiry: integer(),
  ...Timestamps,
})

export const AccountStateTable = pgTable("account_state", {
  id: integer().primaryKey(),
  active_account_id: text().references(() => AccountTable.id, { onDelete: "set null" }),
  active_org_id: text(),
})

// LEGACY
export const ControlAccountTable = pgTable(
  "control_account",
  {
    email: text().notNull(),
    url: text().notNull(),
    access_token: text().notNull(),
    refresh_token: text().notNull(),
    token_expiry: integer(),
    active: integer()
      .notNull()
      .$default(() => 0),
    ...Timestamps,
  },
  (table) => [primaryKey({ columns: [table.email, table.url] })],
)
