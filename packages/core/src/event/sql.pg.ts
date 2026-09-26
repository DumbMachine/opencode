import { pgTable, text, index, uniqueIndex, bigint } from "drizzle-orm/pg-core"
import { Event } from "@opencode/schema/event"

export const EventSequenceTable = pgTable("event_sequence", {
  aggregate_id: text().notNull().primaryKey(),
  seq: bigint({ mode: "number" }).notNull(),
  owner_id: text(),
})

export const EventTable = pgTable(
  "event",
  {
    id: text().$type<Event.ID>().primaryKey(),
    aggregate_id: text()
      .notNull()
      .references(() => EventSequenceTable.aggregate_id, { onDelete: "cascade" }),
    seq: bigint({ mode: "number" }).notNull(),
    created: bigint({ mode: "number" }).notNull().default(0),
    type: text().notNull(),
    data: text().$type<Record<string, unknown>>().notNull(),
  },
  (table) => [
    uniqueIndex("event_aggregate_seq_idx").on(table.aggregate_id, table.seq),
    index("event_aggregate_type_seq_idx").on(table.aggregate_id, table.type, table.seq),
  ],
)
