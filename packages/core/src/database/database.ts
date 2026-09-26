export * as Database from "./database.js"

import { EffectDrizzleSqlite } from "./drizzle.js"
import { sqliteLayer, supportsForeignKeyToggle, supportsTuningPragmas } from "#sqlite"
import { PgAsyncDatabase, PgAsyncSession, PgDialect } from "drizzle-orm/pg-core"
import { PgClient } from "@effect/sql-pg"
import { Config, Context, Effect, Layer, Redacted, Schema, Semaphore } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { Global } from "@opencode/util/global"
import { isAbsolute, join } from "path"
import { DatabaseMigration } from "./migration.js"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import * as DatabaseConfig from "./config.js"

type Sql = SqlClient

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
type DatabaseShape = Effect.Success<typeof makeDatabase>

class EffectPgSession extends PgAsyncSession {
  constructor(public client: Sql, dialect: PgDialect) {
    super(dialect)
  }
  override prepareQuery(query: any, mode: any, _name?: any, mapper?: any): any {
    return new EffectPgPreparedQuery(this.client, query, mode, mapper)
  }
  override transaction(): any {
    return Promise.reject(new Error("PostgreSQL transactions run through the database client"))
  }
}

class EffectPgPreparedQuery {
  constructor(
    public client: Sql,
    public query: any,
    public fields: any,
    public customResultMapper: any,
  ) {}

  executeEffect() {
    const self = this
    return Effect.gen(function* () {
      const stmt = self.client.unsafe(self.query.sql, self.query.params ?? [])
      if (self.customResultMapper && self.fields === "arrays") {
        const rows = yield* stmt.values
        return self.customResultMapper(rows)
      }
      return yield* stmt.withoutTransform
    })
  }
}

const numericPgFields = new Set([
  "active",
  "admitted_seq",
  "baseline_seq",
  "count",
  "position",
  "revision",
  "seq",
  "time_active",
  "time_archived",
  "time_completed",
  "time_idle",
  "time_viewed",
  "time_created",
  "time_updated",
  "time_initialized",
  "time_used",
  "time_suspended",
  "time_compacting",
  "tokens_cache_read",
  "tokens_cache_write",
  "tokens_input",
  "tokens_output",
  "tokens_reasoning",
])

const jsonPgFields = new Set([
  "model",
  "fork_boundary",
  "summary_diffs",
  "metadata",
  "revert",
  "permission",
  "commands",
  "data",
  "payload",
  "value",
  "initial_values",
  "current_values",
  "binding",
])

function normalizePgRow(row: unknown): unknown {
  if (!row || typeof row !== "object" || Array.isArray(row)) return row
  const next: Record<string, unknown> = { ...(row as Record<string, unknown>) }
  for (const [key, value] of Object.entries(next)) {
    if (numericPgFields.has(key) && typeof value === "string" && /^-?\d+$/.test(value)) {
      next[key] = Number(value)
    } else if (jsonPgFields.has(key) && typeof value === "string") {
      try {
        next[key] = JSON.parse(value)
      } catch {}
    }
  }
  return next
}

function normalizePgRows(rows: unknown): unknown {
  if (!Array.isArray(rows)) return rows
  return rows.map(normalizePgRow)
}

function compatPostgresDb(client: Sql): DatabaseShape {
  const dialect = new PgDialect()
  const session = new EffectPgSession(client, dialect)
  const pgDb = new PgAsyncDatabase(dialect, session, {}) as any
  pgDb.$client = client

  function toQuery(query: any): { sql: string; params: unknown[] } {
    if (typeof query === "string") return { sql: query, params: [] }
    if (query && typeof query === "object" && typeof query.toSQL === "function") {
      return query.toSQL()
    }
    const sequel = query && typeof query === "object" && "getSQL" in query ? query.getSQL() : query
    if (sequel && typeof sequel === "object" && ("queryChunks" in sequel || "toQuery" in sequel)) {
      return dialect.sqlToQuery(sequel)
    }
    return { sql: String(query), params: [] }
  }

  function executeQuery(qb: any) {
    if (qb && typeof qb === "object" && typeof qb._prepare === "function") {
      const prep = qb._prepare()
      if (prep && typeof prep.executeEffect === "function") {
        return prep.executeEffect().pipe(
          Effect.tapError((err) => {
            console.error("[Postgres executeQuery Error]", prep.query?.sql, prep.query?.params, err)
            return Effect.void
          }),
        )
      }
    }
    const { sql, params } = toQuery(qb)
    return client.unsafe(sql, params).withoutTransform.pipe(
      Effect.tapError((err) => {
        console.error("[Postgres executeQuery Error]", sql, params, err)
        return Effect.void
      }),
    )
  }

  function patchEffectQuery(qb: any): any {
    if (!qb || typeof qb !== "object") return qb
    if (!("get" in qb)) {
      qb.get = () =>
        Effect.gen(function* () {
          const rows = yield* executeQuery(qb)
          return normalizePgRow(rows[0])
        })
    }
    if (!("all" in qb)) {
      qb.all = () =>
        Effect.gen(function* () {
          const rows = yield* executeQuery(qb)
          return normalizePgRows(rows)
        })
    }
    if (!("run" in qb)) {
      qb.run = () =>
        Effect.gen(function* () {
          yield* executeQuery(qb)
        }).pipe(Effect.asVoid)
    }
    for (const method of [
      "from",
      "where",
      "values",
      "set",
      "returning",
      "onConflictDoUpdate",
      "onConflictDoNothing",
      "innerJoin",
      "leftJoin",
      "rightJoin",
      "orderBy",
      "limit",
      "offset",
      "groupBy",
      "having",
    ]) {
      const orig = qb[method]
      if (typeof orig === "function" && !orig.__opencodePgCompatPatched) {
        const wrapped = function (this: any, ...args: any[]) {
          return patchEffectQuery(orig.apply(this, args))
        }
        ;(wrapped as any).__opencodePgCompatPatched = true
        qb[method] = wrapped
      }
    }
    return qb
  }

  const origSelect = pgDb.select.bind(pgDb)
  pgDb.select = (...args: any[]) => patchEffectQuery(origSelect(...args))
  for (const method of ["insert", "update", "delete"] as const) {
    const orig = pgDb[method]?.bind(pgDb)
    if (orig) pgDb[method] = (...args: any[]) => patchEffectQuery(orig(...args))
  }

  pgDb.run = (query: unknown) =>
    Effect.gen(function* () {
      const { sql, params } = toQuery(query)
      yield* client.unsafe(sql, params).withoutTransform.pipe(
        Effect.tapError((err) => {
          console.error("[Postgres run Error]", sql, params, err)
          return Effect.void
        }),
      )
    }).pipe(Effect.asVoid)

  pgDb.all = (query: unknown) =>
    Effect.gen(function* () {
      const { sql, params } = toQuery(query)
      const rows = yield* client.unsafe(sql, params).withoutTransform.pipe(
        Effect.tapError((err) => {
          console.error("[Postgres all Error]", sql, params, err)
          return Effect.void
        }),
      )
      return normalizePgRows(rows)
    })

  pgDb.get = (query: unknown) =>
    Effect.gen(function* () {
      const { sql, params } = toQuery(query)
      const rows = yield* client.unsafe(sql, params).withoutTransform.pipe(
        Effect.tapError((err) => {
          console.error("[Postgres get Error]", sql, params, err)
          return Effect.void
        }),
      )
      return normalizePgRow(rows[0])
    })

  pgDb.transaction = (transaction: any) =>
    Effect.gen(function* () {
      return yield* client.withTransaction(transaction(pgDb))
    })

  return pgDb as unknown as DatabaseShape
}


export interface Interface {
  db: DatabaseShape
  config?: DatabaseConfig.Config
}

export const Options = Schema.Struct({
  path: Schema.optional(Schema.String),
})
export type Options = typeof Options.Type

export class Service extends Context.Service<Service, Interface>()("@opencode/storage/Database") {}

const sqliteService = (lock: Effect.Effect<Semaphore.Semaphore>) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const db = yield* makeDatabase
      if (supportsTuningPragmas) {
        yield* db.run("PRAGMA journal_mode = WAL")
        yield* db.run("PRAGMA synchronous = NORMAL")
        yield* db.run("PRAGMA busy_timeout = 5000")
        yield* db.run("PRAGMA cache_size = -64000")
        yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")
      }
      if (supportsForeignKeyToggle) yield* db.run("PRAGMA foreign_keys = ON")
      const semaphore = yield* lock
      yield* semaphore.withPermit(DatabaseMigration.apply(db))
      return { db }
    }).pipe(Effect.orDie),
  )

const postgresService = (url: string) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const client = yield* SqlClient
      const db = compatPostgresDb(client)
      yield* client.unsafe(`
        CREATE OR REPLACE FUNCTION json_extract(doc text, path text) RETURNS text IMMUTABLE LANGUAGE sql AS $$
          SELECT (doc::jsonb #>> string_to_array(trim(leading '$.' from path), '.'))
        $$;
      `).withoutTransform
      const semaphore = yield* Effect.succeed(lockFor(url))
      yield* semaphore.withPermit(DatabaseMigration.apply(db, "postgres"))
      return {
        db,
        config: {
          dialect: "postgres" as const,
          sqliteFilename: DatabaseConfig.sqliteDefaultPath(),
          postgresUrl: url,
        },
      }
    }).pipe(Effect.orDie),
  )

const locks = new Map<string, Semaphore.Semaphore>()

function lockFor(filename: string) {
  const existing = locks.get(filename)
  if (existing) return existing
  const lock = Semaphore.makeUnsafe(1)
  locks.set(filename, lock)
  return lock
}

export function sqliteDatabaseLayer(filename: string): Layer.Layer<Service> {
  const lock = filename === ":memory:" ? Semaphore.make(1) : Effect.succeed(lockFor(filename))
  return sqliteService(lock).pipe(Layer.provide(sqliteLayer({ filename }))) as unknown as Layer.Layer<Service>
}

export function postgresDatabaseLayer(url: string): Layer.Layer<Service> {
  const pgClientLayer = PgClient.layerConfig({
    url: Config.succeed(Redacted.make(url)),
    ssl: Config.succeed(false),
    maxConnections: Config.succeed(10),
  }).pipe(Layer.orDie)
  return postgresService(url).pipe(Layer.provide(pgClientLayer)) as unknown as Layer.Layer<Service>
}

export function layer(options: Options = { path: ":memory:" }) {
  return Layer.unwrap(
    Effect.gen(function* () {
      const config = yield* DatabaseConfig.loadEffect
      if (config.dialect === "postgres" && config.postgresUrl) return postgresDatabaseLayer(config.postgresUrl)
      const filename = options.path ?? config.sqliteFilename ?? ":memory:"
      if (filename === ":memory:" || isAbsolute(filename)) return sqliteDatabaseLayer(filename)
      const global = yield* Global.Service
      return sqliteDatabaseLayer(join(global.data, filename))
    }),
  )
}

export const layerFromClient: Layer.Layer<Service, never, SqlClient | Global.Service> = sqliteService(Semaphore.make(1))

export function configured(options?: Options) {
  return makeGlobalNode({ service: Service, layer: layer(options), deps: [Global.node] })
}

export function configuredClient(client: Layer.Layer<SqlClient>) {
  return makeGlobalNode({
    service: Service,
    layer: layerFromClient.pipe(Layer.provide(client)),
    deps: [Global.node],
  })
}

export const node = configured({ path: ":memory:" })
