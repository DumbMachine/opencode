import { and, asc, desc, eq, gte, or, sql } from "drizzle-orm"
import { Effect, Option, Schema } from "effect"
import { Database } from "../database/database.js"
import { MessageDecodeError } from "./error.js"
import { SessionMessage } from "./message.js"
import { SessionSchema } from "./schema.js"
import { Instructions } from "../instructions/index.js"
import { InstructionState } from "./instruction-state.js"
import { SessionProviderContext } from "./provider-context.js"
import { SessionInputRequirements } from "./input-requirements.js"
import { SessionMessageTable, SessionTerminalControlTable, SessionTerminalInvocationTable } from "./sql.js"
import { SessionPublicationProvenance } from "./publication-provenance.js"

type DatabaseService = Database.Interface["db"]

const decode = Schema.decodeUnknownEffect(SessionMessage.Info)

/**
 * Which completed compactions bound a history read. Local summaries always do. Native
 * windows do for model-neutral readers (`latest`), never for the original transcript
 * (`local`), and only when the target model can replay them (a provenance).
 */
export type Boundary = "latest" | "local" | SessionProviderContext.Provenance

const replayable = (message: SessionMessage.Info, boundary: Boundary) =>
  !SessionProviderContext.isCheckpoint(message) ||
  boundary === "latest" ||
  (boundary !== "local" && SessionProviderContext.compatible(message.providerContext.provenance, boundary))

export const latestCompaction = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  boundary: Boundary,
) {
  return yield* db
    .select({ seq: SessionMessageTable.seq })
    .from(SessionMessageTable)
    .where(
      and(
        eq(SessionMessageTable.session_id, sessionID),
        eq(SessionMessageTable.type, "compaction"),
        sql`json_extract(${SessionMessageTable.data}, '$.status') = 'completed'`,
        boundary === "latest"
          ? undefined
          : or(
              sql`json_extract(${SessionMessageTable.data}, '$.providerContext') is null`,
              boundary === "local"
                ? undefined
                : and(
                    ...Object.entries(boundary).map(
                      ([key, value]) =>
                        sql`json_extract(${SessionMessageTable.data}, ${`$.providerContext.provenance.${key}`}) = ${value}`,
                    ),
                  ),
            ),
      ),
    )
    .orderBy(desc(SessionMessageTable.seq))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
})

export const decodeMessageRow = (row: typeof SessionMessageTable.$inferSelect) =>
  decode({ ...row.data, id: row.id, type: row.type }).pipe(
    Effect.tap((message) =>
      SessionProviderContext.isCheckpoint(message)
        ? SessionProviderContext.validate(message.providerContext)
        : Effect.void,
    ),
    Effect.mapError(
      () =>
        new MessageDecodeError({
          sessionID: SessionSchema.ID.make(row.session_id),
          messageID: SessionMessage.ID.make(row.id),
        }),
    ),
  )

const messageEntries = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  boundary: Boundary,
) {
  const compaction = yield* latestCompaction(db, sessionID, boundary)
  const rows = yield* db
    .select()
    .from(SessionMessageTable)
    .where(
      and(
        eq(SessionMessageTable.session_id, sessionID),
        compaction ? gte(SessionMessageTable.seq, compaction.seq) : undefined,
      ),
    )
    .orderBy(asc(SessionMessageTable.seq))
    .all()
    .pipe(Effect.orDie)
  const entries = yield* Effect.forEach(rows, (row) =>
    decodeMessageRow(row).pipe(
      Effect.map((message) => ({ seq: row.seq, message, provenance: row.publication_provenance })),
    ),
  )
  // Re-expansion may cross a native checkpoint whose completion already advanced the instruction
  // epoch: the baseline supersedes the chronological updates before it. Forks seed their baseline
  // at sequence 0 but retain parent sequences, so the copied checkpoint still retires them.
  const native = entries.findLast((entry) => SessionProviderContext.isCheckpoint(entry.message))
  // Skipped native checkpoints are not textual summaries. Their original transcript remains available.
  return entries.filter(
    (entry) =>
      !(entry.message.type === "system" && native && entry.seq < native.seq) && replayable(entry.message, boundary),
  )
})

// A UI-control omission needs the exact private observation/batch binding and
// measured row revision. An arbitrary/stale marker is never a history filter.
const modelEntries = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  entries: Effect.Success<ReturnType<typeof messageEntries>>,
) {
  const controls = yield* db
    .select()
    .from(SessionTerminalControlTable)
    .where(eq(SessionTerminalControlTable.session_id, sessionID))
    .all()
    .pipe(Effect.orDie)
  return entries.filter((entry) => {
    const terminal = entry.provenance?.terminalControl
    if (!terminal) return true
    if (
      entry.provenance?.revision !== SessionPublicationProvenance.revision(entry.message) ||
      !controls.some(
        (control) =>
          control.message_id === entry.message.id &&
          control.unit_id === terminal.unitID &&
          control.unit_digest === terminal.unitDigest,
      )
    )
      throw new Error("Terminal control history binding is unavailable")
    return false
  })
})

export const load = Effect.fn("SessionHistory.load")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  boundary: Boundary,
) {
  return (yield* messageEntries(db, sessionID, boundary)).map((entry) => entry.message)
})

/** Input ownership survives compaction; direct event notes continue the existing input. */
export const latestInput = Effect.fn("SessionHistory.latestInput")(function* (
  db: Pick<DatabaseService, "select">,
  sessionID: SessionSchema.ID,
) {
  const row = yield* db
    .select()
    .from(SessionMessageTable)
    .where(
      and(
        eq(SessionMessageTable.session_id, sessionID),
        or(
          eq(SessionMessageTable.type, "user"),
          and(
            eq(SessionMessageTable.type, "synthetic"),
            sql`coalesce(json_extract(${SessionMessageTable.data}, '$.origin'), '') <> 'event'`,
          ),
        ),
      ),
    )
    .orderBy(desc(SessionMessageTable.seq))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
  if (!row) return undefined
  const message = yield* decodeMessageRow(row)
  return message.type === "user" || message.type === "synthetic" ? message : undefined
})

export const entriesForRunner = Effect.fn("SessionHistory.entriesForRunner")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  instructions: Instructions.List,
  boundary: Boundary,
) {
  return yield* db
    .transaction(() =>
      Effect.gen(function* () {
        const messages = yield* messageEntries(db, sessionID, boundary)
        return {
          initial: yield* InstructionState.initial(db, sessionID, instructions),
          entries: yield* modelEntries(db, sessionID, messages),
          input: yield* latestInput(db, sessionID),
        }
      }),
    )
    .pipe(Effect.orDie)
})

export const preview = Effect.fn("SessionHistory.preview")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  instructions: Instructions.List,
  boundary: Boundary,
) {
  const observed = yield* Instructions.read(instructions)
  return yield* db
    .transaction(() =>
      Effect.gen(function* () {
        const messages = yield* modelEntries(db, sessionID, yield* messageEntries(db, sessionID, boundary))
        // An active assistant may contain an unresolved tool call, so only preview the settled prefix.
        const unsettled = messages.findIndex(
          (entry) => entry.message.type === "assistant" && entry.message.time.completed === undefined,
        )
        const settled = unsettled === -1 ? messages : messages.slice(0, unsettled)
        const assembled = yield* InstructionState.preview(db, sessionID, instructions, observed)
        return {
          initial: assembled.initial,
          messages: settled.map((entry) => entry.message),
          requiresDisclosure:
            settled.some(
              (entry) =>
                entry.message.type === "user" && SessionInputRequirements.auxiliaryBlocked(entry.message.capabilities),
            ) || (yield* requiresDisclosure(db, sessionID)),
          instructionUpdate: assembled.update,
        }
      }),
    )
    .pipe(Effect.catch((error) => (error instanceof Instructions.InitializationBlocked ? error : Effect.die(error))))
})

/** Returns the session's first user message. */
export const firstUserMessage = Effect.fn("SessionHistory.firstUserMessage")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
) {
  const row = yield* db
    .select()
    .from(SessionMessageTable)
    .where(and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.type, "user")))
    .orderBy(asc(SessionMessageTable.seq))
    .get()
    .pipe(Effect.orDie)
  if (!row) return undefined
  const message = yield* decodeMessageRow(row).pipe(Effect.orElseSucceed(() => undefined))
  return message?.type === "user" ? message : undefined
})

export * as SessionHistory from "./history.js"

/** Auxiliary requests cannot borrow another input's service or ignore unsupported history. */
export const requiresDisclosure = Effect.fn("SessionHistory.requiresDisclosure")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
) {
  const rows = yield* db
    .select({
      requirements: sql<string | null>`json_extract(${SessionMessageTable.data}, '$.capabilities')`,
      nodeType: sql<string | null>`json_type(${SessionMessageTable.data}, '$.capabilities')`,
    })
    .from(SessionMessageTable)
    .where(and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.type, "user")))
    .all()
    .pipe(Effect.orDie)
  const decode = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Array(Schema.String)))
  return rows.some((row) => {
    if (row.nodeType === null) return false
    if (row.nodeType !== "array") return true
    const required = decode(row.requirements)
    return Option.isNone(required) || SessionInputRequirements.auxiliaryBlocked(required.value)
  })
})

// The private barrier precedes pub2, so an uncertain publication or restart
// cannot dispatch the old input again. It carries no model/source authority.
export const terminalInput = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  inputID: SessionMessage.ID,
) {
  const terminal = yield* db
    .select({ inputID: SessionTerminalControlTable.input_id })
    .from(SessionTerminalControlTable)
    .where(
      and(eq(SessionTerminalControlTable.session_id, sessionID), eq(SessionTerminalControlTable.input_id, inputID)),
    )
    .get()
    .pipe(Effect.orDie)
  if (terminal) return true
  return (
    (yield* db
      .select({ inputID: SessionTerminalInvocationTable.input_id })
      .from(SessionTerminalInvocationTable)
      .where(
        and(
          eq(SessionTerminalInvocationTable.session_id, sessionID),
          eq(SessionTerminalInvocationTable.input_id, inputID),
        ),
      )
      .limit(1)
      .get()
      .pipe(Effect.orDie)) !== undefined
  )
})
