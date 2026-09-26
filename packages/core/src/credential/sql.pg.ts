import { integer, pgTable, text } from "drizzle-orm/pg-core"
import { Timestamps } from "../database/schema.sql.pg.js"
import type { Credential } from "../credential.js"

export const CredentialTable = pgTable("credential", {
  id: text().$type<Credential.ID>().primaryKey(),
  integration_id: text().$type<Credential.Info["integrationID"]>(),
  label: text().notNull(),
  value: text().$type<Credential.Value>().notNull(),
  connector_id: text(),
  method_id: text(),
  active: integer(),
  ...Timestamps,
})
