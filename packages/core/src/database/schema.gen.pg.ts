import { Effect } from "effect"
import type { DatabaseMigration } from "./migration.js"

const schema: Omit<DatabaseMigration.Migration, "id"> = {
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE OR REPLACE FUNCTION json_extract(doc text, path text) RETURNS text IMMUTABLE LANGUAGE sql AS $$
          SELECT (doc::jsonb #>> string_to_array(trim(leading '$.' from path), '.'))
        $$;
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "account" (
          "id" text PRIMARY KEY,
          "email" text NOT NULL,
          "url" text NOT NULL,
          "access_token" text NOT NULL,
          "refresh_token" text NOT NULL,
          "token_expiry" integer,
          "time_created" BIGINT NOT NULL,
          "time_updated" BIGINT NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "account_state" (
          "id" SERIAL PRIMARY KEY,
          "active_account_id" text REFERENCES "account"("id") ON DELETE SET NULL,
          "active_org_id" text
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "control_account" (
          "email" text NOT NULL,
          "url" text NOT NULL,
          "access_token" text NOT NULL,
          "refresh_token" text NOT NULL,
          "token_expiry" integer,
          "active" integer DEFAULT 0 NOT NULL,
          "time_created" BIGINT NOT NULL,
          "time_updated" BIGINT NOT NULL,
          CONSTRAINT "control_account_pk" PRIMARY KEY("email", "url")
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "credential" (
          "id" text PRIMARY KEY,
          "integration_id" text,
          "label" text NOT NULL,
          "value" text NOT NULL,
          "connector_id" text,
          "method_id" text,
          "active" integer,
          "time_created" BIGINT NOT NULL,
          "time_updated" BIGINT NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "event_sequence" (
          "aggregate_id" text PRIMARY KEY,
          "seq" BIGINT NOT NULL,
          "owner_id" text
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "event" (
          "id" text PRIMARY KEY,
          "aggregate_id" text NOT NULL REFERENCES "event_sequence"("aggregate_id") ON DELETE CASCADE,
          "seq" BIGINT NOT NULL,
          "created" BIGINT DEFAULT 0 NOT NULL,
          "type" text NOT NULL,
          "data" text NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "kv" (
          "key" text PRIMARY KEY,
          "value" text NOT NULL,
          "time_created" BIGINT NOT NULL,
          "time_updated" BIGINT NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "project" (
          "id" text PRIMARY KEY,
          "worktree" text NOT NULL,
          "vcs" text,
          "name" text,
          "icon_url" text,
          "icon_url_override" text,
          "icon_color" text,
          "time_created" BIGINT NOT NULL,
          "time_updated" BIGINT NOT NULL,
          "time_initialized" BIGINT,
          "sandboxes" text NOT NULL,
          "commands" text
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "permission" (
          "id" text PRIMARY KEY,
          "project_id" text NOT NULL REFERENCES "project"("id") ON DELETE CASCADE,
          "action" text NOT NULL,
          "resource" text NOT NULL,
          "time_created" BIGINT NOT NULL,
          "time_updated" BIGINT NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "project_directory" (
          "project_id" text NOT NULL REFERENCES "project"("id") ON DELETE CASCADE,
          "directory" text NOT NULL,
          "type" text,
          "strategy" text,
          "time_created" BIGINT NOT NULL,
          CONSTRAINT "project_directory_pk" PRIMARY KEY("project_id", "directory")
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "session_v2" (
          "id" text PRIMARY KEY,
          "project_id" text NOT NULL REFERENCES "project"("id") ON DELETE CASCADE,
          "workspace_id" text,
          "parent_id" text,
          "fork_session_id" text,
          "fork_boundary" text,
          "slug" text NOT NULL,
          "directory" text NOT NULL,
          "path" text,
          "title" text,
          "version" text NOT NULL,
          "share_url" text,
          "summary_additions" integer,
          "summary_deletions" integer,
          "summary_files" integer,
          "summary_diffs" text,
          "metadata" text,
          "cost" real DEFAULT 0 NOT NULL,
          "tokens_input" integer DEFAULT 0 NOT NULL,
          "tokens_output" integer DEFAULT 0 NOT NULL,
          "tokens_reasoning" integer DEFAULT 0 NOT NULL,
          "tokens_cache_read" integer DEFAULT 0 NOT NULL,
          "tokens_cache_write" integer DEFAULT 0 NOT NULL,
          "revert" text,
          "permission" text,
          "agent" text,
          "model" text,
          "time_created" BIGINT NOT NULL,
          "time_updated" BIGINT NOT NULL,
          "time_compacting" BIGINT,
          "time_archived" BIGINT,
          "time_suspended" BIGINT,
          "resume_attempts" integer DEFAULT 0 NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "instruction_blob" (
          "hash" text PRIMARY KEY,
          "value" text
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "instruction_entry" (
          "session_id" text NOT NULL REFERENCES "session_v2"("id") ON DELETE CASCADE,
          "key" text NOT NULL,
          "value" text,
          "removed" integer DEFAULT 0 NOT NULL,
          "time_created" BIGINT NOT NULL,
          "time_updated" BIGINT NOT NULL,
          CONSTRAINT "instruction_entry_pk" PRIMARY KEY("session_id", "key")
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "instruction_state" (
          "session_id" text PRIMARY KEY REFERENCES "session_v2"("id") ON DELETE CASCADE,
          "epoch_start" BIGINT NOT NULL,
          "through_seq" BIGINT NOT NULL,
          "initial_values" text NOT NULL,
          "current_values" text NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "session_inbox" (
          "id" text PRIMARY KEY,
          "session_id" text NOT NULL REFERENCES "session_v2"("id") ON DELETE CASCADE,
          "type" text NOT NULL,
          "payload" text NOT NULL,
          "delivery" text NOT NULL,
          "enqueued_seq" BIGINT NOT NULL,
          "time_created" BIGINT NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "session_message" (
          "id" text PRIMARY KEY,
          "session_id" text NOT NULL REFERENCES "session_v2"("id") ON DELETE CASCADE,
          "type" text NOT NULL,
          "seq" BIGINT NOT NULL,
          "time_created" BIGINT NOT NULL,
          "time_updated" BIGINT NOT NULL,
          "data" text NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "session_pending" (
          "id" text PRIMARY KEY,
          "session_id" text NOT NULL REFERENCES "session_v2"("id") ON DELETE CASCADE,
          "type" text NOT NULL,
          "data" text NOT NULL,
          "delivery" text,
          "admitted_seq" BIGINT NOT NULL,
          "time_created" BIGINT NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "workspace" (
          "id" text PRIMARY KEY,
          "provider" text NOT NULL,
          "binding" text NOT NULL,
          "created_at" BIGINT NOT NULL,
          "last_used_at" BIGINT NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS "worktree" (
          "project_id" text NOT NULL REFERENCES "project"("id") ON DELETE CASCADE,
          "directory" text NOT NULL,
          "strategy" text,
          "time_created" BIGINT NOT NULL,
          CONSTRAINT "worktree_pk" PRIMARY KEY("project_id", "directory")
        );
      `)
      yield* tx.run(`CREATE UNIQUE INDEX IF NOT EXISTS "event_aggregate_seq_idx" ON "event" ("aggregate_id", "seq");`)
      yield* tx.run(`CREATE INDEX IF NOT EXISTS "event_aggregate_type_seq_idx" ON "event" ("aggregate_id", "type", "seq");`)
      yield* tx.run(
        `CREATE UNIQUE INDEX IF NOT EXISTS "permission_project_action_resource_idx" ON "permission" ("project_id", "action", "resource");`,
      )
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS "session_inbox_session_delivery_seq_idx" ON "session_inbox" ("session_id", "delivery", "enqueued_seq");`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX IF NOT EXISTS "session_inbox_session_enqueued_seq_idx" ON "session_inbox" ("session_id", "enqueued_seq");`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX IF NOT EXISTS "session_message_session_seq_idx" ON "session_message" ("session_id", "seq");`,
      )
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS "session_message_session_type_seq_idx" ON "session_message" ("session_id", "type", "seq");`,
      )
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS "session_message_session_time_created_id_idx" ON "session_message" ("session_id", "time_created", "id");`,
      )
      yield* tx.run(`CREATE INDEX IF NOT EXISTS "session_message_time_created_idx" ON "session_message" ("time_created");`)
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS "session_pending_session_delivery_seq_idx" ON "session_pending" ("session_id", "delivery", "admitted_seq");`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX IF NOT EXISTS "session_pending_session_compaction_idx" ON "session_pending" ("session_id") WHERE "type" = 'compaction';`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX IF NOT EXISTS "session_pending_session_admitted_seq_idx" ON "session_pending" ("session_id", "admitted_seq");`,
      )
      yield* tx.run(`CREATE INDEX IF NOT EXISTS "session_v2_project_idx" ON "session_v2" ("project_id");`)
      yield* tx.run(`CREATE INDEX IF NOT EXISTS "session_v2_workspace_idx" ON "session_v2" ("workspace_id");`)
      yield* tx.run(`CREATE INDEX IF NOT EXISTS "session_v2_parent_idx" ON "session_v2" ("parent_id");`)
      yield* tx.run(
        `CREATE INDEX IF NOT EXISTS "session_v2_time_suspended_idx" ON "session_v2" ("time_suspended") WHERE "time_suspended" is not null;`,
      )
    })
  },
}

export default schema
