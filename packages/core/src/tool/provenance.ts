export * as ToolProvenance from "./provenance.js"

import { Context, Effect, Option } from "effect"
import type { Tool } from "../tool.js"
import { definition } from "./runtime.js"
import { SessionDisclosure } from "../session/disclosure.js"

// Declarations belong to compiled producers. Plugin input/configuration cannot
// set registry metadata to acquire one. The retained value is the exact wire
// definition, so later schema/description changes invalidate the observation.
class Compiled extends Context.Service<Compiled, { readonly definitions: boolean; readonly transforms: boolean }>()(
  "@opencode/ToolProvenance/Compiled",
) {}
const definitions = new WeakMap<object, string>()
const executors = new WeakSet<object>()

export const compiled = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  input: {
    readonly definitions?: boolean
    readonly transforms?: boolean
  },
) =>
  effect.pipe(
    Effect.provideService(Compiled, { definitions: input.definitions === true, transforms: input.transforms === true }),
  )

export const registration = Effect.serviceOption(Compiled).pipe(Effect.map(Option.getOrUndefined))
export const recordDefinition = (tool: Tool.Info) => {
  definitions.set(tool.execute, SessionDisclosure.digest(definition(tool)))
}
export const publicDefinition = (tool: Tool.Info) =>
  definitions.get(tool.execute) === SessionDisclosure.digest(definition(tool))
export const resultProducer = (tool: Tool.Info) => {
  executors.add(tool.execute)
  return tool
}
export const hasResultProducer = (tool: Tool.Info) => executors.has(tool.execute)

interface CatalogSource {
  readonly execution: object
  readonly server: string
  readonly tool: string
  readonly revision: string
}
const catalogs = new WeakMap<object, { readonly revision: string; readonly source: CatalogSource }>()
const snapshotSources = new WeakMap<object, readonly CatalogSource[]>()
const snapshots = new WeakMap<object, boolean>()
const namespaces = new Set<string>()
export const recordNamespace = (namespace: Tool.Namespace) => namespaces.add(SessionDisclosure.digest(namespace))
export const publicNamespace = (namespace: Tool.Namespace) => namespaces.has(SessionDisclosure.digest(namespace))
export const catalogDefinition = (tool: Tool.Info, source: CatalogSource) => {
  catalogs.set(tool.execute, { revision: SessionDisclosure.digest(definition(tool)), source: Object.freeze(source) })
  executors.add(tool.execute)
  return tool
}
export const knownDefinition = (tool: Tool.Info) =>
  publicDefinition(tool) || catalogs.get(tool.execute)?.revision === SessionDisclosure.digest(definition(tool))
export const recordSnapshot = <A extends object>(snapshot: A, complete: boolean, tools: readonly Tool.Info[]) => {
  snapshots.set(snapshot, complete)
  snapshotSources.set(
    snapshot,
    tools.flatMap((tool) => {
      const catalog = catalogs.get(tool.execute)
      return catalog && catalog.revision === SessionDisclosure.digest(definition(tool)) ? [catalog.source] : []
    }),
  )
  return snapshot
}
export const sources = (snapshot: object) => snapshotSources.get(snapshot)
export const snapshotComplete = (snapshot: object) => snapshots.get(snapshot) === true
