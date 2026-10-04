// OpenCode plugin: TUI toasts for the opencode-gemini-proxy failover proxy.
// Install: copy to ~/.config/opencode/plugins/ (global) or .opencode/plugins/
// (per-project). See README.md.
//
// Dual entry point — one file runs on both majors:
//   - OpenCode V2: default export `{ id, setup(ctx) }` (same shape as
//     Plugin.define from @opencode/plugin). Toasts run in the TUI context via
//     ctx.ui.toast.show; the server-side import of this file no-ops because
//     it has no UI.
//   - OpenCode V1 (1.18.29+): reads `server(input, options)` off the same
//     default export and uses client.tui.showToast as before.

import { appendFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const BASE_URL =(process.env.GEMINI_PROXY_URL || "http://127.0.0.1:8085").replace(/\/+$/, "")
const TOAST_DONE = process.env.GEMINI_PROXY_TOAST_DONE === "1"
const VARIANT = {
  model_switch: "info",
  slow_response: "warning",
  waiting: "warning",
  quota_daily: "error",
  key_rejected: "error",
  request_failed: "error",
  request_done: "info",
}

const SEVERITY = {
  request_done: 0,
  model_switch: 1,
  slow_response: 2,
  waiting: 2,
  quota_daily: 3,
  key_rejected: 3,
  request_failed: 3,
}

const MIN_SEVERITY = {
  off: 99,
  none: 99,
  false: 99,
  error: 3,
  errors: 3,
  warning: 2,
  warn: 2,
  info: 1,
  all: 0,
  true: 1,
}

function resolveMinSeverity(options) {
  // Option sources:
  // 1. Plugin option in opencode.json(c): toasts = false | "off" | "error" | ...
  // 2. Environment variable: GEMINI_PROXY_TOASTS=off | false | error
  // Never call client.* while resolving: OpenCode loads plugins while loading
  // its config, so awaiting client.config.get() at init deadlocks startup.
  let rawSetting = options?.toasts
  if (rawSetting === undefined && process.env.GEMINI_PROXY_TOASTS !== undefined) {
    rawSetting = process.env.GEMINI_PROXY_TOASTS
  }
  const toastSetting = String(rawSetting ?? "info").toLowerCase()
  return MIN_SEVERITY[toastSetting] ?? 1
}

function makeShouldToast(minSev) {
  return (type) => {
    if (minSev >= 99) return false
    if (type === "request_done" && !TOAST_DONE) return false
    const typeSev = SEVERITY[type] ?? 1
    return typeSev >= minSev
  }
}

// The TUI owns stderr, so debug lines also go to a file when
// GEMINI_PROXY_DEBUG is set: <tmpdir>/gemini-proxy-plugin.log
function dbg(message) {
  if (!process.env.GEMINI_PROXY_DEBUG) return
  try {
    console.error(`[gemini-proxy-plugin] ${message}`)
    appendFileSync(join(tmpdir(), "gemini-proxy-plugin.log"), `${new Date().toISOString()} ${message}\n`)
  } catch {}
}

// Shared watcher: SSE stream from the proxy + session-idle health check.
// emit() shows a toast in the host environment; log() is best-effort.
// signal aborts the stream loop (returned as stop()).
function createWatcher({ emit, log, shouldToast, signal }) {
  const recent = new Map() // msg -> last shown ts; 10s dedupe window
  const dedupedToast = (message, variant, duration) => {
    const now = Date.now()
    if (now - (recent.get(message) || 0) < 10000) return
    recent.set(message, now)
    emit(message, variant, duration)
  }
  let lastUnreachableToast = 0

  const aborted = () => signal?.aborted === true

  async function connect(sinceMs, backoffMs) {
    try {
      const res = await fetch(`${BASE_URL}/api/events?since=${sinceMs}`, { signal })
      if (!res.ok || !res.body) throw new Error(`events stream ${res.status}`)
      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buf = ""
      backoffMs = 2000
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })
        let idx
        while ((idx = buf.indexOf("\n\n")) !== -1) {
          const lines = buf.slice(0, idx).split("\n")
          buf = buf.slice(idx + 2)
          const data = lines.filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim())
          if (!data.length) continue
          try {
            const evt = JSON.parse(data.join(""))
            if (evt.msg && shouldToast(evt.type)) {
              dedupedToast(evt.msg, VARIANT[evt.type] || "info", evt.type === "waiting" ? 8000 : undefined)
            }
          } catch (err) {
            log("debug", "bad SSE event", { err: String(err) })
          }
        }
      }
      throw new Error("events stream closed")
    } catch (err) {
      if (aborted()) return
      log("debug", "events stream error, reconnecting", { err: String(err) })
    }
    await new Promise((r) => setTimeout(r, backoffMs))
    if (aborted()) return
    connect(Date.now(), Math.min(backoffMs * 2, 30000))
  }
  connect(Date.now(), 2000)

  async function healthCheck() {
    try {
      const res = await fetch(`${BASE_URL}/health`).catch(() => null)
      if (!res || !res.ok) {
        const now = Date.now()
        if (now - lastUnreachableToast > 5 * 60000) {
          lastUnreachableToast = now
          if (shouldToast("request_failed")) {
            emit(`Gemini proxy not running at ${BASE_URL}. Start it with: npm start (or npx opencode-gemini-proxy)`, "error")
          }
        }
        return
      }
      const health = await res.json()
      const cooling =
        (health.models || []).some((m) => m.cooling) || (health.keys || []).some((k) => k.cooling)
      if ((health.status === "degraded" || cooling) && shouldToast("waiting")) {
        dedupedToast(`Gemini proxy: ${health.status || "degraded"} (some models/keys cooling)`, "warning")
      }
    } catch (err) {
      log("debug", "session.idle health check failed", { err: String(err) })
    }
  }

  return { healthCheck, stop: () => { try { signal?.abort() } catch {} } }
}

// --- OpenCode V1 entry (>= 1.18.29 reads `server` off the default export) ---
async function server(input, options = {}) {
  const client = input?.client
  const minSev = resolveMinSeverity(options)
  const shouldToast = makeShouldToast(minSev)
  const log = (level, message, extra) => {
    try {
      const payload = { service: "gemini-proxy-plugin", level, message, extra }
      client?.app?.log?.({ ...payload, body: payload })
    } catch {}
  }
  const emit = (message, variant, duration) => {
    try {
      const payload = { message, variant, duration }
      client?.tui?.showToast?.({ ...payload, body: payload })
    } catch (err) {
      log("debug", "showToast failed", { err: String(err) })
    }
  }
  const ac = typeof AbortController !== "undefined" ? new AbortController() : undefined
  const watcher = createWatcher({ emit, log, shouldToast, signal: ac?.signal })
  dbg(`V1 server() entry loaded`)
  return {
    event: async ({ event }) => {
      try {
        if (event?.type !== "session.idle") return
        await watcher.healthCheck()
      } catch (err) {
        log("debug", "session.idle health check failed", { err: String(err) })
      }
    },
    dispose: async () => watcher.stop(),
  }
}

// --- OpenCode V2 entry -------------------------------------------------------
async function setup(ctx) {
  // This file is imported twice by V2: once as the server entrypoint and once
  // as the TUI entrypoint. Toasts only exist in the TUI context, and all of
  // this plugin's observable behavior is toasts, so the server import no-ops.
  if (!ctx?.ui) {
    dbg("setup() server context — no-op")
    return
  }
  dbg(`setup() TUI context — options=${JSON.stringify(ctx.options ?? {})}`)

  const minSev = resolveMinSeverity(ctx.options)
  const shouldToast = makeShouldToast(minSev)
  const log = (level, message, extra) => {
    try {
      const payload = { service: "gemini-proxy-plugin", level, message, extra }
      ctx.client?.app?.log?.({ ...payload, body: payload })
    } catch {}
  }
  const emit = (message, variant, duration) => {
    try {
      ctx.ui.toast.show({ message, variant, duration })
      dbg(`toast ${variant}: ${message}`)
    } catch (err) {
      log("debug", "toast.show failed", { err: String(err) })
    }
  }
  const ac = typeof AbortController !== "undefined" ? new AbortController() : undefined
  const watcher = createWatcher({ emit, log, shouldToast, signal: ac?.signal })

  // session.idle -> proxy health check (V2 events are an AsyncIterable stream)
  const eventApi = ctx.client?.event ?? ctx.event
  if (eventApi?.subscribe) {
    ;(async () => {
      try {
        for await (const ev of eventApi.subscribe(ac ? { signal: ac.signal } : undefined)) {
          if (ev?.type === "session.idle") await watcher.healthCheck()
        }
      } catch (err) {
        if (!ac?.signal?.aborted) log("debug", "event stream ended", { err: String(err) })
      }
    })()
  }

  return () => watcher.stop()
}

const GeminiProxy = {
  id: "gemini-proxy",
  setup,
  server,
}

export const GeminiProxyPlugin = server
export const Plugin = server
export default GeminiProxy
