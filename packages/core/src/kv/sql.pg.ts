import { pgTable, text } from "drizzle-orm/pg-core"
import { Timestamps } from "../database/schema.sql.pg.js"
import type { KV } from "../kv.js"

export const KVTable = pgTable("kv", {
  key: text().primaryKey(),
  value: text({ mode: "json" }).$type<KV.Value>().notNull(),
  ...Timestamps,
})
