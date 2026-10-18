import { expect, test } from "bun:test"
import { Context, Deferred, Effect, Exit, Layer, Schema } from "effect"
import { eq, sql } from "drizzle-orm"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Event } from "@opencode/schema/event"
import { EventSequenceTable, EventTable } from "@opencode/core/event/sql"
import { Global } from "@opencode/util/global"
import { tmpdir } from "./fixture/tmpdir"

const url = process.env.OPENCODE_TEST_DATABASE_URL
const Message = Bus.durable({
  type: "test.publication.once",
  durable: { version: 1, aggregate: "id" },
  schema: { id: Schema.String, text: Schema.String, state: Schema.Record(Schema.String, Schema.Unknown) },
})

test.skipIf(!url)(
  "acknowledges exact PostgreSQL once retries and rolls back losing separate-pool races",
  async () => {
    if (!new URL(url!).pathname.startsWith("/native_sharing_"))
      throw new Error("dedicated native_sharing_ database required")
    await using temporary = await tmpdir()
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const connect = () =>
            Effect.gen(function* () {
              const context = yield* Layer.build(
                AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node]), [
                  Database.node.replace(Database.postgresDatabaseLayer(url!)),
                  Bus.node.replace(Bus.configured({ persist: true })),
                ]),
              )
              return { bus: Context.get(context, Bus.Service), db: Context.get(context, Database.Service).db }
            })
          const first = yield* connect(),
            second = yield* connect()
          expect(yield* first.db.get(sql`SELECT pg_backend_pid() AS pid`)).not.toEqual(
            yield* second.db.get(sql`SELECT pg_backend_pid() AS pid`),
          )
          yield* first.db
            .run(
              "CREATE TABLE IF NOT EXISTS event_once_postgres_probe (aggregate_id text NOT NULL, value text NOT NULL)",
            )
            .pipe(Effect.orDie)
          for (let index = 0; index < 2; index++) {
            const id = Event.ID.create()
            const events = [
              [
                Message,
                { id, text: "witness", state: { optional: undefined, nested: { value: "kept", missing: undefined } } },
                { id: Event.ID.create(), created: Date.now() },
              ],
              [Message, { id, text: "content", state: {} }, { id: Event.ID.create(), created: Date.now() + 1 }],
            ] as const
            try {
              expect((yield* first.bus.publishAllOnce(events)).inserted).toBe(true)
              const recovered = yield* second.bus.publishAllOnce(events)
              expect(recovered.inserted).toBe(false)
              expect(typeof recovered.events[0].created).toBe("number")
              expect(recovered.events[0].data.state).toEqual({ nested: { value: "kept" } })
              expect(
                yield* first.db.select().from(EventTable).where(eq(EventTable.aggregate_id, id)).all(),
              ).toHaveLength(2)
            } finally {
              yield* first.db
                .delete(EventSequenceTable)
                .where(eq(EventSequenceTable.aggregate_id, id))
                .run()
                .pipe(Effect.orDie)
            }
          }
          const id = Event.ID.create(),
            barrier = yield* Deferred.make<void>()
          let arrived = 0,
            notifications = 0
          const unlisten = []
          for (const connection of [first, second]) {
            yield* connection.bus.project(Message, (event) =>
              Effect.gen(function* () {
                if (event.data.id !== id) return
                yield* connection.db
                  .run(
                    sql`INSERT INTO event_once_postgres_probe (aggregate_id, value) VALUES (${id}, ${event.data.text})`,
                  )
                  .pipe(Effect.orDie)
                if (event.data.text !== "witness") return
                arrived++
                if (arrived === 2) yield* Deferred.succeed(barrier, undefined)
                yield* Deferred.await(barrier)
              }),
            )
            unlisten.push(
              yield* connection.bus.listen((event) =>
                Effect.sync(() => {
                  if ((event.data as { id?: string }).id === id) notifications++
                }),
              ),
            )
          }
          const events = [
            [Message, { id, text: "witness", state: {} }, { id: Event.ID.create(), created: Date.now() }],
            [Message, { id, text: "content", state: {} }, { id: Event.ID.create(), created: Date.now() + 1 }],
          ] as const
          try {
            const results = yield* Effect.all(
              [first.bus.publishAllOnce(events).pipe(Effect.exit), second.bus.publishAllOnce(events).pipe(Effect.exit)],
              { concurrency: "unbounded" },
            )
            expect(results.filter(Exit.isSuccess)).toHaveLength(1)
            expect(results.filter(Exit.isFailure)).toHaveLength(1)
            expect(notifications).toBe(2)
            expect(
              yield* first.db.all(sql`SELECT value FROM event_once_postgres_probe WHERE aggregate_id=${id}`),
            ).toHaveLength(2)
            expect(yield* first.db.select().from(EventTable).where(eq(EventTable.aggregate_id, id)).all()).toHaveLength(
              2,
            )
            expect((yield* first.bus.publishAllOnce(events)).inserted).toBe(false)
            expect((yield* second.bus.publishAllOnce(events)).inserted).toBe(false)
            expect(notifications).toBe(2)
          } finally {
            for (const stop of unlisten) yield* stop
            yield* first.db.run(sql`DELETE FROM event_once_postgres_probe WHERE aggregate_id=${id}`).pipe(Effect.orDie)
            yield* first.db
              .delete(EventSequenceTable)
              .where(eq(EventSequenceTable.aggregate_id, id))
              .run()
              .pipe(Effect.orDie)
          }
        }),
      ).pipe(Effect.provideService(Global.Service, Global.make({ data: temporary.path }))),
    )
  },
  20000,
)
