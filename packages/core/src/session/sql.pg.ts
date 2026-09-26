import { pgTable, text, integer, index, primaryKey, real, uniqueIndex, bigint } from "drizzle-orm/pg-core"
import { sql } from "drizzle-orm"
import { directoryColumn, pathColumn } from "../database/path.pg.js"
import { ProjectTable } from "../project/sql.pg.js"
import type { SessionMessage } from "./message.js"
import type { SessionInbox } from "./inbox.js"
import type { FileDiff } from "@opencode/schema/file-diff"
import { PermissionV1 } from "../v1/permission.js"
import { Project } from "../project.js"
import type { SessionSchema } from "./schema.js"
import { Workspace } from "../workspace.js"
import { Timestamps } from "../database/schema.sql.pg.js"
import type { Instruction } from "@opencode/schema/instruction"
import type { Session } from "@opencode/schema/session"
import type { CompactionPayload, MovePayload, SyntheticPayload, UserPayload } from "@opencode/schema/session-inbox"
import type { RevertV1 } from "@opencode/schema/session-revert"
import type { Schema } from "effect"

type SessionMessageData = Omit<(typeof SessionMessage.Info)["Encoded"], "type" | "id">

export const SessionTable = pgTable(
  "session_v2",
  {
    id: text().$type<SessionSchema.ID>().primaryKey(),
    project_id: text()
      .$type<Project.ID>()
      .notNull()
      .references(() => ProjectTable.id, { onDelete: "cascade" }),
    workspace_id: text().$type<Workspace.ID>(),
    parent_id: text().$type<SessionSchema.ID>(),
    fork_session_id: text().$type<SessionSchema.ID>(),
    fork_boundary: text().$type<Session.ForkBoundary>(),
    slug: text().notNull(),
    directory: directoryColumn().notNull(),
    path: pathColumn(),
    title: text(),
    version: text().notNull(),
    share_url: text(),
    summary_additions: integer(),
    summary_deletions: integer(),
    summary_files: integer(),
    summary_diffs: text().$type<FileDiff.LegacyInfo[]>(),
    metadata: text().$type<Record<string, unknown>>(),
    cost: real().notNull().default(0),
    tokens_input: integer().notNull().default(0),
    tokens_output: integer().notNull().default(0),
    tokens_reasoning: integer().notNull().default(0),
    tokens_cache_read: integer().notNull().default(0),
    tokens_cache_write: integer().notNull().default(0),
    revert: text().$type<Session.Revert | RevertV1>(),
    permission: text().$type<PermissionV1.Ruleset>(),
    agent: text(),
    model: text().$type<{
      id: string
      providerID: string
      variant?: string
    }>(),
    ...Timestamps,
    time_idle: bigint({ mode: "number" }),
    time_viewed: bigint({ mode: "number" }),
    idle_outcome: text().$type<NonNullable<Session.Info["outcome"]>>(),
    time_compacting: bigint({ mode: "number" }),
    time_archived: bigint({ mode: "number" }),
    /** The execution claim timestamp (historical column name; see SessionStore.claim). */
    time_suspended: bigint({ mode: "number" }),
    resume_attempts: integer().notNull().default(0),
  },
  (table) => [
    index("session_v2_project_idx").on(table.project_id),
    index("session_v2_workspace_idx").on(table.workspace_id),
    index("session_v2_parent_idx").on(table.parent_id),
    index("session_v2_time_suspended_idx")
      .on(table.time_suspended)
      .where(sql`${table.time_suspended} is not null`),
  ],
)

export const SessionMessageTable = pgTable(
  "session_message",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    type: text().$type<SessionMessage.Type>().notNull(),
    seq: bigint({ mode: "number" }).notNull(),
    ...Timestamps,
    data: text().notNull().$type<SessionMessageData>(),
  },
  (table) => [
    uniqueIndex("session_message_session_seq_idx").on(table.session_id, table.seq),
    index("session_message_session_type_seq_idx").on(table.session_id, table.type, table.seq),
    index("session_message_session_time_created_id_idx").on(table.session_id, table.time_created, table.id),
    index("session_message_time_created_idx").on(table.time_created),
  ],
)

export const SessionPendingTable = pgTable(
  "session_pending",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    type: text().$type<SessionInbox.Info["type"]>().notNull(),
    data: text().$type<UserPayload | SyntheticPayload | Record<string, never>>().notNull(),
    delivery: text().$type<SessionInbox.Delivery>(),
    admitted_seq: bigint({ mode: "number" }).notNull(),
    time_created: bigint({ mode: "number" })
      .notNull()
      .$default(() => Date.now()),
  },
  (table) => [
    index("session_pending_session_delivery_seq_idx").on(table.session_id, table.delivery, table.admitted_seq),
    uniqueIndex("session_pending_session_compaction_idx")
      .on(table.session_id)
      .where(sql`${table.type} = 'compaction'`),
    uniqueIndex("session_pending_session_admitted_seq_idx").on(table.session_id, table.admitted_seq),
  ],
)

export const SessionInboxTable = pgTable(
  "session_inbox",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    type: text().$type<SessionInbox.Info["type"]>().notNull(),
    payload: text().$type<UserPayload | SyntheticPayload | CompactionPayload | MovePayload>().notNull(),
    delivery: text().$type<SessionInbox.Delivery>().notNull(),
    enqueued_seq: bigint({ mode: "number" }).notNull(),
    time_created: bigint({ mode: "number" })
      .notNull()
      .$default(() => Date.now()),
  },
  (table) => [
    index("session_inbox_session_delivery_seq_idx").on(table.session_id, table.delivery, table.enqueued_seq),
    uniqueIndex("session_inbox_session_enqueued_seq_idx").on(table.session_id, table.enqueued_seq),
  ],
)

export const InstructionEntryTable = pgTable(
  "instruction_entry",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    key: text().notNull(),
    value: text().$type<Schema.Json>(),
    removed: integer().notNull().default(0),
    ...Timestamps,
  },
  (table) => [primaryKey({ columns: [table.session_id, table.key] })],
)

export const InstructionBlobTable = pgTable("instruction_blob", {
  hash: text().$type<Instruction.Hash>().primaryKey(),
  value: text().$type<Schema.Json>(),
})

export const InstructionStateTable = pgTable("instruction_state", {
  session_id: text()
    .$type<SessionSchema.ID>()
    .primaryKey()
    .references(() => SessionTable.id, { onDelete: "cascade" }),
  epoch_start: bigint({ mode: "number" }).notNull(),
  through_seq: bigint({ mode: "number" }).notNull(),
  initial_values: text().notNull().$type<Instruction.Values>(),
  current_values: text().notNull().$type<Instruction.Values>(),
})
