import { expect } from "bun:test"
import { LanguageModel, LLM, LLMEvent } from "@opencode/ai"
import { OpenAIChat } from "@opencode/ai/protocols/openai-chat"
import { TestLLM } from "@opencode/ai/testing"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { EventTable } from "@opencode/core/event/sql"
import { Location } from "@opencode/core/location"
import { Permission } from "@opencode/core/permission"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { SessionStep } from "@opencode/core/session/runner/step"
import { SessionTable } from "@opencode/core/session/sql"
import { Snapshot } from "@opencode/core/snapshot"
import { Tool } from "@opencode/core/tool"
import { ToolOutput } from "@opencode/core/tool-output"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { eq } from "drizzle-orm"
import { Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"

const it = testEffect(
  Layer.merge(
    AppNodeBuilder.build(
      LayerNode.group([Database.node, Bus.node, SessionProjector.node, ToolOutput.node, Agent.node, Permission.node]),
      [
        Bus.node.replace(Bus.configured({ persist: true })),
        Location.node.replace(
          Layer.succeed(Location.Service, Location.Service.of(location({ directory: AbsolutePath.make("/project") }))),
        ),
      ],
    ),
    TestLLM.testLayer(),
  ),
)

it.live("interrupts pending permission tools after the provider stream ends", () =>
  Effect.gen(function* () {
    const db = (yield* Database.Service).db
    const bus = yield* Bus.Service
    const permission = yield* Permission.Service
    const llm = yield* TestLLM.Test
    const agents = yield* Agent.Service
    const agent = Agent.ID.make("interrupted-permissions")
    yield* agents.transform((editor) =>
      editor.update(agent, (value) => {
        value.permissions = [{ action: "resource_role", resource: "*", effect: "ask" }]
      }),
    )
    const sessionID = Session.ID.create()
    const assistantMessageID = SessionMessage.ID.create()
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "interrupted-permissions",
        directory: "/project",
        agent,
        version: "test",
      })
      .run()
    const model = SessionRunnerModel.resolved(
      LanguageModel.make({ id: "test-model", provider: "test", route: OpenAIChat.route }),
      {
        capabilities: { tools: true, input: ["text"], output: ["text"] },
        limit: { context: 100_000, output: 1_000 },
        cost: [],
      },
    )
    const pending = yield* Deferred.make<void>()
    const streamed = yield* Deferred.make<void>()
    let asks = 0
    yield* bus.listen((event) => {
      if (Schema.is(Permission.Event.Asked)(event) && event.data.sessionID === sessionID && ++asks === 2)
        return Deferred.succeed(pending, undefined).pipe(Effect.asVoid)
      if (Schema.is(SessionEvent.Step.Streamed)(event) && event.data.sessionID === sessionID)
        return Deferred.succeed(streamed, undefined).pipe(Effect.asVoid)
      return Effect.void
    })
    yield* llm.push(
      TestLLM.complete(
        { reason: { normalized: "stop" } },
        LLMEvent.toolCall({ id: "permission-one", name: "test", input: {} }),
        LLMEvent.toolCall({ id: "permission-two", name: "test", input: {} }),
      ),
    )
    const steps = yield* SessionStep.make.pipe(
      Effect.provide(Layer.mock(Snapshot.Service)({ capture: () => Effect.succeed(Snapshot.ID.make("unchanged")) })),
    )
    let executions = 0
    const fiber = yield* steps
      .attempt({
        isLocationClosed: () => false,
        sessionID,
        assistantMessageID,
        agent,
        model,
        prepared: {
          retry: () => Effect.void,
          request: LLM.request({ model: model.model, prompt: "Wait for approval" }),
          options: {},
          executeTool: (call) =>
            permission
              .assert({
                action: "resource_role",
                resources: [call.call.id],
                save: [],
                metadata: {},
                sessionID,
                agent,
                source: { type: "tool", messageID: assistantMessageID, id: call.call.id },
              })
              .pipe(
                Effect.mapError((cause) => new Tool.Error({ message: cause.message })),
                Effect.andThen(
                  Effect.sync(() => {
                    executions++
                    return { content: [{ type: "text" as const, text: "Approved action" }] }
                  }),
                ),
              ),
        },
        retry: () => Effect.succeed({ retry: false }),
        recoverContinuation: false,
        recoverOverflow: Effect.succeed(false),
      })
      .pipe(Effect.forkChild)
    yield* Deferred.await(pending)
    yield* Deferred.await(streamed)
    expect(yield* permission.forSession(sessionID)).toHaveLength(2)
    yield* Fiber.interrupt(fiber).pipe(Effect.timeout("2 seconds"))
    const interrupted = yield* Fiber.await(fiber)
    expect(Exit.hasInterrupts(interrupted)).toBe(true)
    expect(yield* permission.forSession(sessionID)).toHaveLength(0)
    expect(executions).toBe(0)
    const events = yield* db
      .select({ type: EventTable.type })
      .from(EventTable)
      .where(eq(EventTable.aggregate_id, sessionID))
      .all()
    expect(events.filter((event) => event.type === "session.tool.success.2")).toHaveLength(0)
  }),
)
