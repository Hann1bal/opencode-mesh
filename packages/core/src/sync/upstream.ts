export * as UpstreamSync from "./upstream.js"

import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http"
import { Context, Effect, Layer, Schedule, Schema, Stream } from "effect"
import { Event } from "@opencode/schema/event"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { httpClient } from "@opencode/util/effect/app-node-platform"
import { Bus, InvalidDurableEventError, latestSequence } from "../bus.js"
import { Database } from "../database/database.js"

/**
 * Live session sharing.
 *
 * This process tails the durable event log of another OpenCode server (the
 * upstream, typically the home machine) and replays every event into the local
 * bus under `ownerID = peer:<id>`. Local projections materialize the upstream
 * sessions as replicas that stream live through the local `/event` feed, so a
 * single GUI can follow sessions owned by many servers.
 *
 * The tail is read-only: replayed aggregates stay owned by the peer, so a local
 * write to a replica is rejected by `strictOwner` instead of silently
 * diverging from the upstream transcript.
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

// Mirrors the session log SSE contract, `Schema.Union([SessionEvent.Durable,
// EventLog.Synced])`, decoded loosely so fields a newer upstream adds do not
// break the tail. The marker is listed first: its literal type keeps the two
// members disjoint under `oneOf`.
const WireEvent = Schema.Struct({
  id: Schema.String,
  type: Schema.String,
  created: Schema.optional(Schema.Finite),
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

const WireFrame = Schema.Union([WireSynced, WireEvent], { mode: "oneOf" })

// SSE frames arrive as `data: <json>` lines split across arbitrary byte chunks.
// One decoder per stream keeps partial multi-byte sequences intact, and the
// accumulator holds the bytes of the frame that has not terminated yet.
const sseData = <E>(bytes: Stream.Stream<Uint8Array, E>) => {
  const decoder = new TextDecoder()
  return bytes.pipe(
    Stream.map((chunk) => decoder.decode(chunk, { stream: true }).replace(/\r\n/g, "\n")),
    Stream.mapAccum(() => "", (buffer, text) => {
      const frames = (buffer + text).split("\n\n")
      const pending = frames.pop() ?? ""
      const data = frames
        .flatMap((frame) => frame.split("\n"))
        .map((line) => (line.startsWith("data:") ? line.slice(5).trim() : ""))
        .filter((payload) => payload.length > 0)
      return [pending, data] as const
    }),
  )
}

// Reconnect with exponential backoff capped at 30s per gap; `min` takes the
// faster schedule so the cap applies per gap rather than in total.
const retrySchedule = Schedule.min([Schedule.exponential("1 second"), Schedule.spaced("30 seconds")]).pipe(Schedule.jittered)

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = readConfig(process.env)
    if (!config) {
      return Service.of({ status: () => Effect.succeed<Status>({ enabled: false, tailed: [] }) })
    }

    const bus = yield* Bus.Service
    const { db } = yield* Database.Service
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
      const decoded = yield* HttpClientResponse.schemaBodyJson(
        Schema.Struct({ data: Schema.Array(Schema.Struct({ id: Schema.String })) }),
      )(response)
      return decoded.data.map((item) => item.id)
    })

    const replayFrame = Effect.fn("UpstreamSync.replayFrame")(function* (sessionID: string, data: string) {
      const frame = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(WireFrame))(data).pipe(
        Effect.catch(() =>
          Effect.die(new UpstreamDivergedError({ sessionID, detail: `undecodable log frame: ${data.slice(0, 180)}` })),
        ),
      )
      if (frame.type === "log.synced") return
      // The log endpoint is keyed by session; a foreign aggregate would map the
      // upstream transcript onto the wrong local session, so refuse it loudly.
      if (frame.durable.aggregateID !== sessionID) {
        return yield* Effect.die(
          new UpstreamDivergedError({
            sessionID,
            detail: `aggregate ${frame.durable.aggregateID} streamed under session ${sessionID}`,
          }),
        )
      }
      if (frame.type === "session.deleted") {
        tailed.delete(sessionID)
        return yield* Effect.logInfo(`UpstreamSync: upstream session ${sessionID} deleted, stopped following`)
      }
      yield* bus.replay(
        {
          id: Event.ID.make(frame.id),
          type: Event.versionedType(frame.type, frame.durable.version),
          created: frame.created ?? 0,
          seq: frame.durable.seq,
          aggregateID: frame.durable.aggregateID,
          data: frame.data,
        },
        { publish: true, ownerID, strictOwner: true },
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
      if (response.status === 404) {
        tailed.delete(sessionID)
        return yield* Effect.logInfo(`UpstreamSync: upstream session ${sessionID} is gone`)
      }
      yield* sseData(response.stream).pipe(Stream.runForEach((data) => replayFrame(sessionID, data)))
    })

    // A rejected replay means the local replica can no longer be trusted against
    // the upstream log: stop following it and surface the conflict instead of
    // retrying into the same rejection. Anything else (network, 5xx) is retried.
    const tailForever = (sessionID: string) =>
      tailOnce(sessionID).pipe(
        Effect.catchDefect((defect) => {
          if (!(defect instanceof InvalidDurableEventError) && !(defect instanceof UpstreamDivergedError)) {
            return Effect.die(defect)
          }
          const detail = defect.message
          const entry = tailed.get(sessionID)
          if (entry) {
            entry.state = "conflicted"
            entry.detail = detail
          }
          return Effect.logError(`UpstreamSync: replica ${sessionID} conflicted: ${detail}`)
        }),
        Effect.retry(retrySchedule),
      )

    const discover = Effect.fn("UpstreamSync.discover")(function* () {
      const sessions = yield* listSessions()
      for (const sessionID of sessions) {
        if (config.sessions && !config.sessions.includes(sessionID)) continue
        if (tailed.has(sessionID)) continue
        tailed.set(sessionID, { state: "following" })
        yield* tailForever(sessionID).pipe(Effect.forkScoped)
      }
    })

    yield* discover().pipe(
      Effect.catchCause((cause) => Effect.logWarning("UpstreamSync: discovery pass failed", { cause })),
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
        return { enabled: true, peerID: config.peerID, tailed: entries } satisfies Status
      }),
    })
  }),
)

export const node = makeGlobalNode({
  service: Service,
  layer,
  deps: [Bus.node, Database.node, httpClient],
})
