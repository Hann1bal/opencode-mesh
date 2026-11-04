export * as UpstreamSync from "./upstream.js"

import { HttpClient, HttpClientRequest } from "effect/http"
import { Context, Effect, Fiber, Layer, Schedule, Schema, Stream } from "effect"
import { Event } from "@opencode/schema/event"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { httpClient } from "@opencode/util/effect/app-node-platform"
import { Bus, latestSequence } from "../bus.js"
import { Database } from "../database/database.js"

/**
 * Live session sharing: this process tails the durable event log of another
 * OpenCode server (the upstream) and replays every event into the local bus with
 * `ownerID = peer:<id>`. The local projections materialize upstream sessions as
 * read-only replicas that stream live through the local `/event` SSE feed, so a
 * single GUI can follow sessions owned by many servers.
 *
 * Disabled unless `OPENCODE_UPSTREAM_URL` and `OPENCODE_UPSTREAM_TOKEN` are set.
 */

export interface Config {
  readonly url: string
  readonly token: string
  readonly peerID: string
  readonly sessions: ReadonlyArray<string> | undefined
}

export class UpstreamAuthError extends Schema.TaggedError<UpstreamAuthError>()("UpstreamSync.AuthError", {
  status: Schema.Number,
}) {}

export class UpstreamDivergedError extends Schema.TaggedError<UpstreamDivergedError>()("UpstreamSync.Diverged", {
  sessionID: Schema.String,
  detail: Schema.String,
}) {}

interface TailedSession {
  readonly fiber: Fiber.Fiber<never, void>
  state: "following" | "conflicted"
  detail?: string
}

export interface Status {
  readonly enabled: boolean
  readonly peerID?: string
  readonly tailed: ReadonlyArray<{
    readonly sessionID: string
    readonly cursor: number
    readonly state: "following" | "conflicted"
  }>
}

export interface Interface {
  readonly status: () => Effect.Effect<Status>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/UpstreamSync") {}

function readConfig(env: Record<string, string | undefined>): Config | undefined {
  const url = env.OPENCODE_UPSTREAM_URL
  const token = env.OPENCODE_UPSTREAM_TOKEN
  if (!url || !token) return undefined
  const sessions = env.OPENCODE_UPSTREAM_SESSIONS?.split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0)
  return {
    url: url.replace(/\/+$/, ""),
    token,
    peerID: env.OPENCODE_UPSTREAM_ID || "home",
    sessions: sessions && sessions.length > 0 ? sessions : undefined,
  }
}

// Wire shape of a durable event as streamed by the upstream session log.
const WireEvent = Schema.Struct({
  id: Schema.String,
  type: Schema.String,
  created: Schema.optional(Schema.Number),
  durable: Schema.Struct({
    aggregateID: Schema.String,
    seq: Schema.Number,
    version: Schema.Number,
  }),
  data: Schema.Record(Schema.String, Schema.Unknown),
})

const WireSynced = Schema.Struct({
  type: Schema.Literal("log.synced"),
  aggregateID: Schema.String,
  seq: Schema.optional(Schema.Number),
})

const decodeUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes, { stream: true })

// Split a byte stream into SSE `data:` payloads, buffering partial frames.
const sseData = (bytes: Stream.Stream<Uint8Array>) =>
  bytes.pipe(
    Stream.map(decodeUtf8),
    Stream.mapAccum("", (buffer, text) => {
      const parts = (buffer + text).split("\n\n")
      return [parts.pop() ?? "", parts]
    }),
    Stream.flatMap((frame) => Stream.fromIterable(frame.split("\n"))),
    Stream.map((line) => (line.startsWith("data:") ? line.slice(5).trim() : "")),
    Stream.filter((data) => data.length > 0),
  )

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = readConfig(process.env)
    if (!config) {
      return Service.of({ status: Effect.succeed({ enabled: false, tailed: [] }) })
    }

    const bus = yield* Bus.Service
    const db = yield* Database.Service
    const http = yield* HttpClient.HttpClient
    const ownerID = `peer:${config.peerID}`
    const tailed = new Map<string, TailedSession>()

    const request = (path: string, accept: string) =>
      HttpClientRequest.get(`${config.url}${path}`).pipe(
        HttpClientRequest.basicAuth("opencode", config.token),
        HttpClientRequest.accept(accept),
      )

    const listSessions = Effect.fn("UpstreamSync.listSessions")(function* () {
      const response = yield* http.execute(request("/api/session?limit=100", "application/json"))
      if (response.status === 401 || response.status === 403) {
        return yield* new UpstreamAuthError({ status: response.status })
      }
      const body = yield* response.json
      const decoded = yield* Schema.decodeUnknownEffect(
        Schema.Struct({ data: Schema.Array(Schema.Struct({ id: Schema.String })) }),
      )(body)
      return decoded.data.map((item) => item.id)
    })

    const replayFrame = Effect.fn("UpstreamSync.replayFrame")(function* (sessionID: string, data: string) {
      const synced = Schema.decodeUnknownOption(WireSynced)(data)
      if (synced._tag === "Some") return
      const event = yield* Schema.decodeUnknownEffect(WireEvent)(data).pipe(
        Effect.catchAll(() => Effect.die(`UpstreamSync: undecodable log frame for ${sessionID}`)),
      )
      // The session log endpoint is keyed by session; a foreign aggregate would
      // corrupt the local mapping, so refuse it loudly.
      if (event.durable.aggregateID !== sessionID) {
        return yield* Effect.die(
          `UpstreamSync: aggregate ${event.durable.aggregateID} streamed under session ${sessionID}`,
        )
      }
      if (event.type === "session.deleted") return yield* Effect.interrupt
      yield* bus
        .replay(
          {
            id: Event.ID.make(event.id),
            type: Event.versionedType(event.type, event.durable.version),
            created: event.created ?? 0,
            seq: event.durable.seq,
            aggregateID: event.durable.aggregateID,
            data: event.data,
          },
          { publish: true, ownerID, strictOwner: true },
        )
        .pipe(
          Effect.catchAllCause((cause) => {
            const detail = String(cause.defects.map((defect) => (defect instanceof Error ? defect.message : defect)).join("; "))
            // A local replica that diverged or was claimed by another owner must
            // stop following; silently skipping would corrupt the transcript.
            if (detail.includes("diverged") || detail.includes("owner mismatch")) {
              return Effect.die(new UpstreamDivergedError({ sessionID, detail }))
            }
            return Effect.logWarning(`UpstreamSync: skipped event for ${sessionID}: ${detail}`)
          }),
        )
    })

    const tailOnce = Effect.fn("UpstreamSync.tailOnce")(function* (sessionID: string) {
      const after = yield* latestSequence(db, sessionID)
      const response = yield* http.execute(
        request(`/api/experimental/session/${sessionID}/log?after=${after}&follow=true`, "text/event-stream"),
      )
      if (response.status === 401 || response.status === 403) {
        return yield* new UpstreamAuthError({ status: response.status })
      }
      if (response.status === 404) return yield* Effect.interrupt
      yield* sseData(response.stream).pipe(Stream.runForEach((data) => replayFrame(sessionID, data)))
    })

    const tailForever = (sessionID: string) =>
      tailOnce(sessionID).pipe(
        Effect.catchAllCause((cause) => {
          const diverged = cause.defects.find((defect) => defect instanceof UpstreamDivergedError)
          if (diverged instanceof UpstreamDivergedError) {
            const entry = tailed.get(sessionID)
            if (entry) {
              entry.state = "conflicted"
              entry.detail = diverged.detail
            }
            return Effect.logError(`UpstreamSync: replica ${sessionID} conflicted: ${diverged.detail}`)
          }
          return Effect.failCause(cause)
        }),
        Effect.retry(
          Schedule.exponential("1 second", { factor: 2, maxInterval: "30 seconds" }).pipe(Schedule.jittered),
        ),
      )

    const discover = Effect.fn("UpstreamSync.discover")(function* () {
      const sessions = yield* listSessions()
      for (const sessionID of sessions) {
        if (config.sessions && !config.sessions.includes(sessionID)) continue
        if (tailed.has(sessionID)) continue
        const fiber = yield* tailForever(sessionID).pipe(Effect.forkScoped)
        tailed.set(sessionID, { fiber, state: "following" })
      }
    })

    yield* discover().pipe(
      Effect.catchAllDefect((defect) => Effect.logWarning(`UpstreamSync: discovery pass failed: ${defect}`)),
      Effect.repeat(Schedule.spaced("10 seconds")),
      Effect.forkScoped,
    )

    yield* Effect.logInfo(`UpstreamSync: following upstream ${config.url} as ${ownerID}`)

    return Service.of({
      status: Effect.gen(function* () {
        const entries = yield* Effect.forEach(
          Array.from(tailed.entries()),
          ([sessionID, entry]) =>
            Effect.gen(function* () {
              const cursor = yield* latestSequence(db, sessionID)
              return { sessionID, cursor, state: entry.state }
            }),
          { concurrency: 1 },
        )
        return { enabled: true, peerID: config.peerID, tailed: entries }
      }),
    })
  }),
)

export const node = makeGlobalNode({
  service: Service,
  layer,
  deps: [Bus.node, Database.node, httpClient],
})
