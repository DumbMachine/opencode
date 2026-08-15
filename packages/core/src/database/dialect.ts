export * as DatabaseDialect from "./dialect.js"

export type Dialect = "sqlite" | "postgres"

export function detect(): Dialect {
  const env = (
    process.env.OPENCODE_DATABASE_DIALECT ??
    process.env.OPENCODE_DATABSE_DIALECT ??
    process.env.OPENCODE_DB_DIALECT ??
    ""
  )
    .toLowerCase()
    .trim()
  if (env === "postgres" || env === "postgresql" || env === "pg") return "postgres"
  const url = (process.env.OPENCODE_DATABASE_URL ?? process.env.DATABASE_URL ?? "").trim()
  if (url.startsWith("postgres://") || url.startsWith("postgresql://")) return "postgres"
  return "sqlite"
}

export function isPostgres(dialect: Dialect): dialect is "postgres" {
  return dialect === "postgres"
}

export function isSQLite(dialect: Dialect): dialect is "sqlite" {
  return dialect === "sqlite"
}
