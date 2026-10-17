import { expect, test } from "bun:test"
import { Context, Deferred, Effect, Fiber, Layer } from "effect"
import { Database } from "@opencode/core/database/database"
import { SessionLifecycle } from "@opencode/core/session/lifecycle"
import { SessionSchema } from "@opencode/core/session/schema"
import { Global } from "@opencode/util/global"
import { tmpdir } from "./fixture/tmpdir"

const url = process.env.OPENCODE_TEST_DATABASE_URL

test.skipIf(!url)(
  "serializes native session activation and retirement across PostgreSQL pools and restart",
  async () => {
    if (!new URL(url!).pathname.startsWith("/native_sharing_"))
      throw new Error("dedicated native_sharing_ database required")
    await using temporary = await tmpdir()
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const first = Context.get(yield* Layer.build(Database.postgresDatabaseLayer(url!)), Database.Service).db
          const second = Context.get(yield* Layer.build(Database.postgresDatabaseLayer(url!)), Database.Service).db
          const retired: SessionSchema.ID[] = []
          for (const [index, sessionID] of [SessionSchema.ID.create(), SessionSchema.ID.create()].entries()) {
            if (index === 1) yield* SessionLifecycle.activate(first, sessionID)
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const finished = yield* Deferred.make<void>()
            const activation = yield* first
              .transaction(() =>
                Effect.gen(function* () {
                  yield* SessionLifecycle.activate(first, sessionID)
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                }),
              )
              .pipe(Effect.forkScoped)
            yield* Deferred.await(entered)
            const retirement = yield* SessionLifecycle.retire(second, sessionID).pipe(
              Effect.andThen(Deferred.succeed(finished, undefined)),
              Effect.forkScoped,
            )
            yield* Effect.sleep("50 millis")
            expect(yield* Deferred.isDone(finished)).toBe(false)
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(activation)
            yield* Fiber.join(retirement)
            expect(yield* Effect.result(SessionLifecycle.activate(first, sessionID))).toMatchObject({
              _tag: "Failure",
              failure: { _tag: "Session.NotFoundError", sessionID },
            })
            retired.push(sessionID)
          }
          const restart = Context.get(yield* Layer.build(Database.postgresDatabaseLayer(url!)), Database.Service).db
          for (const sessionID of retired) {
            yield* SessionLifecycle.retire(restart, sessionID)
            expect(yield* Effect.result(SessionLifecycle.activate(restart, sessionID))).toMatchObject({
              _tag: "Failure",
              failure: { _tag: "Session.NotFoundError", sessionID },
            })
          }
          yield* SessionLifecycle.activate(restart, SessionSchema.ID.create())
        }),
      ).pipe(Effect.provideService(Global.Service, Global.make({ data: temporary.path }))),
    )
  },
  15000,
)
