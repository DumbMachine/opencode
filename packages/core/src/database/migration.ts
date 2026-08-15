export * as DatabaseMigration from "./migration.js"

import { sql } from "drizzle-orm"
import { Effect, Semaphore } from "effect"
import { supportsForeignKeyToggle } from "#sqlite"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { migrations } from "./migration.gen.js"
import sqliteSchema from "./schema.gen.js"
import pgSchema from "./schema.gen.pg.js"
import type { Dialect } from "./dialect.js"
import { Global } from "@opencode-ai/util/global"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0]
const lock = Semaphore.makeUnsafe(1)

export type Migration = {
  id: string
  foreignKeys?: boolean
  up: (tx: Transaction) => Effect.Effect<void, unknown, Global.Service>
}

function listTablesQuery(dialect: Dialect) {
  if (dialect === "postgres") {
    return sql`SELECT table_name AS name FROM information_schema.tables WHERE table_schema = 'public' AND substr(table_name, 1, 1) <> '_'`
  }
  return sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND substr(name, 1, 1) <> '_'`
}

function drizzleMigrationTableQuery(dialect: Dialect) {
  if (dialect === "postgres") {
    return sql`SELECT table_name AS name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ${"__drizzle_migrations"}`
  }
  return sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"__drizzle_migrations"}`
}

function seedMigrationQuery(dialect: Dialect) {
  if (dialect === "postgres") {
    return sql`
      INSERT INTO ${sql.identifier("migration")} (id, time_completed)
      SELECT name, ${Date.now()}
      FROM ${sql.identifier("__drizzle_migrations")}
      WHERE name IS NOT NULL
      ON CONFLICT (id) DO NOTHING
    `
  }
  return sql`
    INSERT OR IGNORE INTO ${sql.identifier("migration")} (id, time_completed)
    SELECT name, ${Date.now()}
    FROM ${sql.identifier("__drizzle_migrations")}
    WHERE name IS NOT NULL
  `
}

function createMigrationTableQuery(dialect: Dialect) {
  if (dialect === "postgres") {
    return sql`CREATE TABLE IF NOT EXISTS ${sql.identifier("migration")} (id TEXT PRIMARY KEY, time_completed BIGINT NOT NULL)`
  }
  return sql`CREATE TABLE IF NOT EXISTS ${sql.identifier("migration")} (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`
}

function insertMigrationQuery(dialect: Dialect, migrationId: string) {
  if (dialect === "postgres") {
    return sql`INSERT INTO ${sql.identifier("migration")} (id, time_completed) VALUES (${migrationId}, ${Date.now()}) ON CONFLICT (id) DO NOTHING`
  }
  return sql`INSERT INTO ${sql.identifier("migration")} (id, time_completed) VALUES (${migrationId}, ${Date.now()})`
}

export function apply(db: Database, dialect: Dialect = "sqlite") {
  return lock.withPermit(
    Effect.gen(function* () {
      const schema = dialect === "postgres" ? pgSchema : sqliteSchema
      // OpenCode owns the unprefixed table namespace. Embedders sharing this
      // database may own underscore-prefixed tables, which bootstrap ignores.
      const tables = yield* db.all<{ name: string }>(listTablesQuery(dialect))
      if (tables.some((table) => table.name === "session" || table.name === "session_v2"))
        return yield* applyOnly(db, migrations, dialect)
      if (tables.length > 0) return yield* Effect.die(new Error("Database is not empty and has no session table"))
      const started = Date.now()
      yield* Effect.logInfo("database schema bootstrap started", { migrations: migrations.length, dialect })
      yield* db.transaction((tx) =>
        Effect.gen(function* () {
          yield* schema.up(tx)
          yield* tx.run(createMigrationTableQuery(dialect))
          yield* Effect.forEach(migrations, (migration) =>
            tx.run(insertMigrationQuery(dialect, migration.id)),
          )
        }),
      )
      yield* Effect.logInfo("database schema bootstrap completed", {
        migrations: migrations.length,
        dialect,
        durationMs: Date.now() - started,
      })
    }),
  )
}

export function applyOnly(db: Database, input: Migration[], dialect: Dialect = "sqlite") {
  return Effect.gen(function* () {
    yield* db.run(createMigrationTableQuery(dialect))
    let completed = new Set(
      (yield* db.all<{ id: string }>(sql`SELECT id FROM ${sql.identifier("migration")}`)).map((row) => row.id),
    )
    if (completed.size === 0) {
      // Existing installs used Drizzle's migration journal. Seed the new
      // journal once so TypeScript migrations don't replay old SQL.
      if (
        yield* db.get(drizzleMigrationTableQuery(dialect))
      ) {
        yield* db.run(seedMigrationQuery(dialect))
        completed = new Set(
          (yield* db.all<{ id: string }>(sql`SELECT id FROM ${sql.identifier("migration")}`)).map((row) => row.id),
        )
      }
    }

    for (const migration of input) {
      if (completed.has(migration.id)) continue
      const started = Date.now()
      yield* Effect.logInfo("database migration started", { migration: migration.id, dialect })
      const apply = db.transaction((tx) =>
        Effect.gen(function* () {
          yield* migration.up(tx)
          yield* tx.run(insertMigrationQuery(dialect, migration.id))
        }),
      )
      if (migration.foreignKeys !== false || dialect === "postgres") {
        yield* apply.pipe(
          Effect.tapError((error) =>
            Effect.logError("database migration failed", {
              migration: migration.id,
              dialect,
              durationMs: Date.now() - started,
              error,
            }),
          ),
        )
        yield* Effect.logInfo("database migration completed", {
          migration: migration.id,
          dialect,
          durationMs: Date.now() - started,
        })
        continue
      }
      // Durable Object SQLite rejects the foreign_keys toggle; the closest
      // allowlisted relaxation is deferring enforcement to transaction commit.
      const relaxForeignKeys = supportsForeignKeyToggle
        ? db.run(sql`PRAGMA foreign_keys = OFF`)
        : db.run(sql`PRAGMA defer_foreign_keys = ON`)
      const restoreForeignKeys = supportsForeignKeyToggle ? db.run(sql`PRAGMA foreign_keys = ON`) : Effect.void
      yield* relaxForeignKeys
      yield* apply.pipe(
        Effect.ensuring(restoreForeignKeys.pipe(Effect.orDie)),
        Effect.tapError((error) =>
          Effect.logError("database migration failed", {
            migration: migration.id,
            dialect,
            durationMs: Date.now() - started,
            error,
          }),
        ),
      )
      yield* Effect.logInfo("database migration completed", {
        migration: migration.id,
        dialect,
        durationMs: Date.now() - started,
      })
    }
  })
}
