export * as SessionContext from "./context.js"

import { Model } from "../model.js"
import { Permission } from "../permission.js"
import { Context, Effect, Layer, Schema } from "effect"
import { Execution } from "@opencode/schema/prompt"
import { Agent } from "../agent.js"
import { CodeModeInstructions } from "../codemode/instructions.js"
import { Database } from "../database/database.js"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { InstructionDiscovery } from "../instruction-discovery.js"
import { Instructions } from "../instructions/index.js"
import { InstructionBuiltIns } from "../instructions/builtins.js"
import { Location } from "../location.js"
import { Mcp } from "../mcp/index.js"
import { McpInstructions } from "../mcp/instructions.js"
import { McpTool } from "../tool/mcp.js"
import { PluginHooks } from "../plugin/hooks.js"
import { ReferenceInstructions } from "../reference/instructions.js"
import { SkillInstructions } from "../skill/instructions.js"
import { Tool } from "../tool.js"
import { AgentNotFoundError } from "./error.js"
import { SessionHistory } from "./history.js"
import { SessionProviderContext } from "./provider-context.js"
import { InstructionState } from "./instruction-state.js"
import { InstructionEntry } from "./instruction-entry.js"
import { SessionMessage } from "./message.js"
import { SessionModelRequest } from "./model-request.js"
import { SessionRunnerModel } from "./runner/model.js"
import { SessionSchema } from "./schema.js"
import { SessionStore } from "./store.js"
import type { ExecutionDisclosure } from "@opencode/schema/execution-disclosure"
import { Hash } from "@opencode/util/hash"
import type { SessionPublicationProvenance } from "./publication-provenance.js"

export interface Selection {
  readonly input?: SessionMessage.User | SessionMessage.Synthetic
  readonly model?: Model.Ref
  readonly session: SessionSchema.Info
  readonly agent: Agent.Selection & { readonly info: Agent.Info }
  readonly instructions: Instructions.List
  readonly tools: Tool.Snapshot
  readonly ephemeral?: string
}

export interface Loaded {
  readonly disclosure?: ExecutionDisclosure.Service
  readonly disclosureRevision?: number
  readonly disclosureCurrent?: () => boolean
  readonly disclosureComponents?: readonly ExecutionDisclosure.Component[]
  readonly disclosureProvenance?: readonly {
    readonly id: SessionMessage.ID
    readonly state: SessionPublicationProvenance.State | null
  }[]
  readonly executionInstructions?: string
  readonly input?: SessionMessage.User | SessionMessage.Synthetic
  readonly session: SessionSchema.Info
  readonly agent: Agent.Selection & { readonly info: Agent.Info }
  readonly model: SessionRunnerModel.Resolved
  readonly initial: string
  readonly ephemeral?: string
  readonly messages: ReadonlyArray<SessionMessage.Info>
  readonly tools: Tool.Snapshot
}

/**
 * Resolves model-request state in two phases: `select` fixes the Session,
 * agent, instruction sources, and tool snapshot; `load` adds the model and
 * active history for that selection. Auxiliary operations resolve only the
 * capabilities they need; request preparation stays separate from selection.
 */
export interface Interface {
  /** Selects the Session, agent, instructions, and tools used by subsequent work. */
  readonly select: (
    sessionID: SessionSchema.ID,
    executionMcp?: Mcp.Execution,
    captured?: {
      readonly input?: SessionMessage.User | SessionMessage.Synthetic
      readonly selection?: Pick<Selection, "session" | "agent" | "model">
    },
  ) => Effect.Effect<Selection, AgentNotFoundError>
  /** Resolves the model and active history for that selection. */
  readonly load: (selection: Selection) => Effect.Effect<Loaded, SessionRunnerModel.Error>
  readonly resolveModel: (
    session: SessionSchema.Info,
  ) => Effect.Effect<SessionRunnerModel.Resolved, SessionRunnerModel.Error>
  /** Selects auxiliary title capabilities without instruction or tool preflight. */
  readonly selectTitle: (
    session: SessionSchema.Info,
    primaryModel?: Model.Ref,
  ) => Effect.Effect<
    | {
        readonly agent: Agent.Info
        readonly primary: SessionRunnerModel.Resolved | undefined
        readonly selected: SessionRunnerModel.Resolved
      }
    | undefined
  >
  readonly request: SessionModelRequest.Interface
}

/** Location-scoped model-context loader for durable Session Steps. */
export class Service extends Context.Service<Service, Interface>()("@opencode/SessionContext") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const agents = yield* Agent.Service
    const builtins = yield* InstructionBuiltIns.Service
    const model = yield* Model.Service
    const db = (yield* Database.Service).db
    const discovery = yield* InstructionDiscovery.Service
    const entries = yield* InstructionEntry.Service
    const location = yield* Location.Service
    const mcpInstructions = yield* McpInstructions.Service
    const mcpTools = yield* McpTool.Service
    const models = yield* SessionRunnerModel.Service
    const request = yield* SessionModelRequest.Service
    const referenceInstructions = yield* ReferenceInstructions.Service
    const skillInstructions = yield* SkillInstructions.Service
    const store = yield* SessionStore.Service
    const registry = yield* Tool.Service
    const hooks = yield* PluginHooks.Service

    const resolveModel = (session: SessionSchema.Info) => models.resolve(session, model.available)

    const selectTitle = Effect.fn("SessionContext.selectTitle")(function* (
      session: SessionSchema.Info,
      primaryModel?: Model.Ref,
    ) {
      const agent = yield* agents.get(Agent.ID.make("title"))
      if (!agent) return
      const primary = yield* resolveModel({ ...session, model: primaryModel ?? session.model }).pipe(
        Effect.orElseSucceed(() => undefined),
      )
      const info = yield* Effect.gen(function* () {
        if (agent.model) return yield* model.get(agent.model.providerID, agent.model.id)
        if (!primary) return
        return yield* model.small(primary.ref.providerID)
      })
      const variant =
        agent.model?.variant ?? MINIMAL_REASONING_VARIANTS.find((id) => info?.variants.some((item) => item.id === id))
      const preferred =
        info &&
        (yield* resolveModel({
          ...session,
          model: Model.Ref.make({
            providerID: info.providerID,
            id: info.id,
            ...(variant ? { variant } : {}),
          }),
        }).pipe(Effect.orElseSucceed(() => undefined)))
      const selected = preferred ?? primary
      if (!selected) return
      return { agent, primary, selected }
    })

    const sharedInstructions = (sessionID: SessionSchema.ID) =>
      Effect.all(
        {
          builtins: builtins.load(sessionID),
          discovery: discovery.load(),
          references: referenceInstructions.load(),
          entries: entries.load(sessionID),
        },
        { concurrency: "unbounded" },
      )
    const select = Effect.fn("SessionContext.select")(function* (
      sessionID: SessionSchema.ID,
      executionMcp?: Mcp.Execution,
      captured?: {
        readonly input?: SessionMessage.User | SessionMessage.Synthetic
        readonly selection?: Pick<Selection, "session" | "agent" | "model">
      },
    ) {
      const current = yield* store.get(sessionID)
      if (!current) return yield* Effect.die(new Error(`Session not found: ${sessionID}`))
      if (current.location.directory !== location.directory || current.location.workspaceID !== location.workspaceID)
        return yield* Effect.interrupt
      const session = captured?.selection?.session ?? current

      const input =
        captured === undefined ? yield* SessionHistory.latestInput(db, sessionID).pipe(Effect.orDie) : captured.input
      const execution = input?.type === "user" ? input.execution : undefined
      if (!session.contextPolicy) yield* mcpTools.flush
      const agent = captured?.selection?.agent ?? (yield* agents.select(execution?.agent ?? session.agent))
      if (!agent.info)
        return yield* new AgentNotFoundError({
          sessionID: session.id,
          agent: execution?.agent ?? session.agent ?? agent.id,
        })
      // Session permissions narrow discovery the same way they narrow the tool snapshot.
      const permissions = session.contextPolicy
        ? [{ action: "*", resource: "*", effect: "deny" as const }]
        : Permission.merge(agent.info.permissions, session.permissions ?? [])
      const executionTools =
        executionMcp && !session.contextPolicy
          ? yield* Effect.gen(function* () {
              const discovered = yield* mcpTools.tools(executionMcp)
              const event = yield* hooks.trigger("tool", "resolve", {
                sessionID,
                agent: agent.id,
                tools: discovered.map((tool) => ({
                  name: tool.name,
                  namespace: tool.options?.namespace,
                  effectiveName: Tool.effectiveName(tool),
                  group: tool.options?.group,
                })),
                renames: [],
              })
              const renamed = new Map(event.renames.map((item) => [item.from, item.to]))
              return discovered.map((tool) => {
                const next = renamed.get(Tool.effectiveName(tool))
                return next ? Tool.rename(tool, next) : tool
              })
            })
          : undefined
      const loaded = yield* Effect.all(
        {
          tools: registry.snapshot(
            permissions,
            executionTools ? { excludeGroups: ["mcp"], tools: executionTools } : undefined,
          ),
          shared: session.contextPolicy
            ? Effect.succeed({
                builtins: Instructions.empty,
                discovery: Instructions.empty,
                references: Instructions.empty,
                entries: Instructions.empty,
              })
            : sharedInstructions(sessionID),
          skills: session.contextPolicy ? Effect.succeed(Instructions.empty) : skillInstructions.load(permissions),
          mcp:
            executionMcp || session.contextPolicy
              ? Effect.succeed(Instructions.empty)
              : mcpInstructions.load(permissions),
          ephemeral:
            executionMcp && !session.contextPolicy
              ? mcpInstructions.ephemeral(executionMcp, permissions)
              : Effect.succeed(undefined),
        },
        { concurrency: "unbounded" },
      )
      if (
        session.contextPolicy &&
        (loaded.tools.definitions.length !== 0 || loaded.tools.codeModeCatalog !== undefined)
      )
        return yield* Effect.die(new Error("Session context policy requires empty tools"))
      return {
        session,
        input,
        model: captured?.selection?.model ?? execution?.model ?? session.model,
        agent: { ...agent, info: agent.info },
        instructions: session.contextPolicy
          ? Instructions.empty
          : Instructions.combine([
              loaded.shared.builtins,
              CodeModeInstructions.make(loaded.tools.codeModeCatalog),
              loaded.shared.discovery,
              loaded.skills,
              loaded.shared.references,
              loaded.mcp,
              loaded.shared.entries,
            ]),
        tools: loaded.tools,
        ephemeral: loaded.ephemeral,
      }
    })

    const load = Effect.fn("SessionContext.load")(function* (selection: Selection) {
      const model = yield* resolveModel({ ...selection.session, model: selection.model })
      const history = yield* SessionHistory.entriesForRunner(
        db,
        selection.session.id,
        selection.instructions,
        SessionProviderContext.provenance(model) ?? "local",
      )
      if (!sameExecution(history.input, selection.input)) return yield* Effect.interrupt
      return {
        session: selection.session,
        agent: selection.agent,
        model,
        initial: history.initial,
        disclosureComponents: [
          { kind: "instructions" as const, id: selection.session.id, revision: Hash.sha256(history.initial) },
          ...history.entries.map((entry) => ({
            kind: "message" as const,
            id: entry.message.id,
            revision: Hash.sha256(JSON.stringify(entry.message)),
          })),
          {
            kind: "agent" as const,
            id: selection.agent.id,
            revision: Hash.sha256(JSON.stringify(selection.agent.info)),
          },
          ...(selection.ephemeral === undefined
            ? []
            : [{ kind: "ephemeral" as const, id: selection.session.id, revision: Hash.sha256(selection.ephemeral) }]),
          ...(history.input?.type === "user" && history.input.execution?.instructions !== undefined
            ? [
                {
                  kind: "input-instructions" as const,
                  id: history.input.id,
                  revision: Hash.sha256(history.input.execution.instructions),
                },
              ]
            : []),
        ],
        ephemeral: selection.ephemeral,
        messages: history.entries.map((entry) => entry.message),
        disclosureProvenance: history.entries.map((entry) => ({ id: entry.message.id, state: entry.provenance })),
        input: history.input,
        executionInstructions: history.input?.type === "user" ? history.input.execution?.instructions : undefined,
        tools: selection.tools,
      }
    })

    return Service.of({ select, load, resolveModel, selectTitle, request })
  }),
)

/** Variant IDs that minimize reasoning output, in preference order. */
const MINIMAL_REASONING_VARIANTS = ["none", "minimal", "low"].map((id) => Model.VariantID.make(id))

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [
    Agent.node,
    Model.node,
    Database.node,
    InstructionBuiltIns.node,
    InstructionDiscovery.node,
    InstructionEntry.node,
    Location.node,
    McpInstructions.node,
    McpTool.node,
    ReferenceInstructions.node,
    SessionRunnerModel.node,
    SessionModelRequest.node,
    SessionStore.node,
    SkillInstructions.node,
    Tool.node,
    PluginHooks.node,
  ],
})

/** Input identity may change at promotion, but its request configuration must match preparation. */
export function sameExecution(
  left: SessionMessage.User | SessionMessage.Synthetic | undefined,
  right: SessionMessage.User | SessionMessage.Synthetic | undefined,
) {
  const a = left?.type === "user" ? left.execution : undefined
  const b = right?.type === "user" ? right.execution : undefined
  return a === undefined || b === undefined ? a === b : equivalentExecution(a, b)
}

const equivalentExecution = Schema.toEquivalence(Execution)
