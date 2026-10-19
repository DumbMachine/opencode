import type { SessionPublicationProvenance } from "./publication-provenance.js"
import { sqliteTable, text, integer, index, primaryKey, real, uniqueIndex } from "drizzle-orm/sqlite-core"
import { sql } from "drizzle-orm"
import { directoryColumn, pathColumn } from "../database/path.js"
import { ProjectTable } from "../project/sql.js"
import type { SessionMessage } from "./message.js"
import type { SessionInbox } from "./inbox.js"
import type { FileDiff } from "@opencode/schema/file-diff"
import type { Permission } from "@opencode/schema/permission"
import type { Project } from "@opencode/schema/project"
import type { SessionSchema } from "./schema.js"
import type { Workspace } from "@opencode/schema/workspace"
import { Timestamps } from "../database/schema.sql.js"
import type { Instruction } from "@opencode/schema/instruction"
import type { Session } from "@opencode/schema/session"
import type { CompactionPayload, MovePayload, SyntheticPayload, UserPayload } from "@opencode/schema/session-inbox"
import type { RevertV1 } from "@opencode/schema/session-revert"
import type { Schema } from "effect"
import type { Event } from "@opencode/schema/event"

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never
type SessionMessageData = DistributiveOmit<(typeof SessionMessage.Info)["Encoded"], "type" | "id">

// Retained independently of session/event cascades so delayed creation cannot
// revive an original after its host has committed deletion.
export const SessionLifecycleTable = sqliteTable("session_lifecycle", {
  id: text().$type<SessionSchema.ID>().primaryKey(),
  retired_at: integer(),
  context_policy: text({ mode: "json" }).$type<Session.Info["contextPolicy"]>(),
})

export const SessionTable = sqliteTable(
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
    fork_boundary: text({ mode: "json" }).$type<Session.ForkBoundary>(),
    slug: text().notNull(),
    directory: directoryColumn().notNull(),
    path: pathColumn(),
    title: text(),
    version: text().notNull(),
    share_url: text(),
    summary_additions: integer(),
    summary_deletions: integer(),
    summary_files: integer(),
    summary_diffs: text({ mode: "json" }).$type<FileDiff.LegacyInfo[]>(),
    metadata: text({ mode: "json" }).$type<Session.Metadata>(),
    context_policy: text({ mode: "json" }).$type<Session.Info["contextPolicy"]>(),
    cost: real().notNull().default(0),
    tokens_input: integer().notNull().default(0),
    tokens_output: integer().notNull().default(0),
    tokens_reasoning: integer().notNull().default(0),
    tokens_cache_read: integer().notNull().default(0),
    tokens_cache_write: integer().notNull().default(0),
    revert: text({ mode: "json" }).$type<Session.Revert | RevertV1>(),
    permission: text({ mode: "json" }).$type<Permission.Ruleset>(),
    agent: text(),
    model: text({ mode: "json" }).$type<{
      id: string
      providerID: string
      variant?: string
    }>(),
    ...Timestamps,
    time_idle: integer(),
    time_viewed: integer(),
    idle_outcome: text().$type<NonNullable<Session.Info["outcome"]>>(),
    time_compacting: integer(),
    time_archived: integer(),
    /** The execution claim timestamp (historical column name; see SessionStore.claim). */
    time_suspended: integer(),
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

export const SessionMessageTable = sqliteTable(
  "session_message",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    type: text().$type<SessionMessage.Type>().notNull(),
    seq: integer().notNull(),
    ...Timestamps,
    publication_provenance: text({ mode: "json" }).$type<SessionPublicationProvenance.State>(),
    data: text({ mode: "json" }).notNull().$type<SessionMessageData>(),
  },
  (table) => [
    uniqueIndex("session_message_session_seq_idx").on(table.session_id, table.seq),
    index("session_message_session_type_seq_idx").on(table.session_id, table.type, table.seq),
    index("session_message_session_time_created_id_idx").on(table.session_id, table.time_created, table.id),
    index("session_message_time_created_idx").on(table.time_created),
  ],
)

export const SessionPendingTable = sqliteTable(
  "session_pending",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    type: text().$type<SessionInbox.Info["type"]>().notNull(),
    data: text({ mode: "json" }).$type<UserPayload | SyntheticPayload | Record<string, never>>().notNull(),
    delivery: text().$type<SessionInbox.Delivery>(),
    admitted_seq: integer().notNull(),
    time_created: integer()
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

export const SessionInboxTable = sqliteTable(
  "session_inbox",
  {
    id: text().$type<SessionMessage.ID>().primaryKey(),
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    type: text().$type<SessionInbox.Info["type"]>().notNull(),
    payload: text({ mode: "json" }).$type<UserPayload | SyntheticPayload | CompactionPayload | MovePayload>().notNull(),
    delivery: text().$type<SessionInbox.Delivery>().notNull(),
    enqueued_seq: integer().notNull(),
    time_created: integer()
      .notNull()
      .$default(() => Date.now()),
  },
  (table) => [
    index("session_inbox_session_delivery_seq_idx").on(table.session_id, table.delivery, table.enqueued_seq),
    uniqueIndex("session_inbox_session_enqueued_seq_idx").on(table.session_id, table.enqueued_seq),
  ],
)

export const InstructionEntryTable = sqliteTable(
  "instruction_entry",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    key: text().notNull(),
    value: text({ mode: "json" }).$type<Schema.Json>(),
    removed: integer({ mode: "boolean" }).notNull().default(false),
    ...Timestamps,
  },
  (table) => [primaryKey({ columns: [table.session_id, table.key] })],
)

export const InstructionBlobTable = sqliteTable("instruction_blob", {
  hash: text().$type<Instruction.Hash>().primaryKey(),
  value: text({ mode: "json" }).$type<Schema.Json>(),
})

export const InstructionStateTable = sqliteTable("instruction_state", {
  session_id: text()
    .$type<SessionSchema.ID>()
    .primaryKey()
    .references(() => SessionTable.id, { onDelete: "cascade" }),
  epoch_start: integer().notNull(),
  through_seq: integer().notNull(),
  initial_values: text({ mode: "json" }).notNull().$type<Instruction.Values>(),
  current_values: text({ mode: "json" }).notNull().$type<Instruction.Values>(),
})

// Private execution barrier. This is not a transcript position or source grant.
export const SessionTerminalControlTable = sqliteTable(
  "session_terminal_control",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    input_id: text().$type<SessionMessage.ID>().notNull(),
    attempt: text().notNull(),
    message_id: text().$type<SessionMessage.ID>().notNull(),
    tool_call_id: text().notNull(),
    producer_epoch: text().notNull(),
    producer_key_id: text().notNull(),
    context_reference: text().notNull(),
    receipt_reference: text().notNull(),
    evidence: text().notNull(),
    unit_id: text().$type<Event.ID>(),
    unit_digest: text(),
    created: integer().notNull(),
  },
  (table) => [primaryKey({ columns: [table.session_id, table.input_id] })],
)

// Pre-transport deny-only intent survives uncertain host completion/recorder failure.
export const SessionTerminalInvocationTable = sqliteTable(
  "session_terminal_invocation",
  {
    session_id: text()
      .$type<SessionSchema.ID>()
      .notNull()
      .references(() => SessionTable.id, { onDelete: "cascade" }),
    input_id: text().$type<SessionMessage.ID>().notNull(),
    invocation_id: text().notNull(),
    context_reference: text().notNull(),
    evidence: text().notNull(),
    created: integer().notNull(),
  },
  (table) => [primaryKey({ columns: [table.session_id, table.input_id, table.invocation_id] })],
)
