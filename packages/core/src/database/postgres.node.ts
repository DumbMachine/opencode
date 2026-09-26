import * as Context from "effect/Context"
import * as Layer from "effect/Layer"
import { PgClient } from "@effect/sql-pg"
import { Config, Redacted } from "effect"

export interface PostgresConfig {
  readonly url: string
  readonly ssl?: boolean
  readonly poolMax?: number
}

export const PostgresConfig = Context.Service<PostgresConfig>(
  "@opencode/core/database/PostgresConfig",
)

export const layer = (config: PostgresConfig) =>
  Layer.merge(
    Layer.succeed(PostgresConfig, config),
    PgClient.layerConfig({
      url: Config.succeed(Redacted.make(config.url)),
      ssl: Config.succeed(config.ssl ?? false),
      maxConnections: Config.succeed(config.poolMax ?? 10),
    }),
  )
