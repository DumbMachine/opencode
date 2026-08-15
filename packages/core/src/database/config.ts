export * as DatabaseConfig from "./config.js"

import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { Global } from "@opencode-ai/util/global"
import { isAbsolute, join } from "path"
import type { Dialect } from "./dialect.js"
import * as DialectModule from "./dialect.js"

export interface Config {
  readonly dialect: Dialect
  readonly sqliteFilename: string
  readonly postgresUrl?: string
}

export const ConfigService = Context.Service<Config>(
  "@opencode/storage/DatabaseConfig",
)

export function sqliteDefaultPath(globalData?: string) {
  const flag = process.env.OPENCODE_DB
  if (flag) {
    if (flag === ":memory:" || isAbsolute(flag)) return flag
    return globalData ? join(globalData, flag) : flag
  }
  return globalData ? join(globalData, "opencode.db") : "opencode.db"
}

export function load(globalData?: string): Config {
  const dialect = DialectModule.detect()

  if (dialect === "postgres") {
    const url = process.env.OPENCODE_DATABASE_URL ?? process.env.DATABASE_URL
    if (!url) {
      throw new Error(
        "OPENCODE_DATABASE_DIALECT=postgres requires OPENCODE_DATABASE_URL to be set",
      )
    }
    return { dialect, sqliteFilename: sqliteDefaultPath(globalData), postgresUrl: url }
  }

  return { dialect, sqliteFilename: sqliteDefaultPath(globalData) }
}

export const defaultLayer = Layer.effect(
  ConfigService,
  Effect.gen(function* () {
    const global = yield* Global.Service
    return load(global.data)
  }),
)

export const loadEffect = Effect.serviceOption(ConfigService).pipe(
  Effect.flatMap((opt) =>
    opt._tag === "Some"
      ? Effect.succeed(opt.value)
      : Effect.gen(function* () {
          const globalOpt = yield* Effect.serviceOption(Global.Service)
          const globalData = globalOpt._tag === "Some" ? globalOpt.value.data : undefined
          return load(globalData)
        }),
  ),
)
