// OpenCode plugin: TUI toasts for the opencode-gemini-proxy failover proxy.
// Install: copy to ~/.config/opencode/plugins/ (global) or .opencode/plugins/
// (per-project). See README.md.

const BASE_URL = (process.env.GEMINI_PROXY_URL || "http://127.0.0.1:8085").replace(/\/+$/, "")
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

/** @type {import("@opencode-ai/plugin").Plugin} */
export const GeminiProxyPlugin = async ({ client }, options = {}) => {
  // Option sources:
  // 1. Plugin option in opencode.json: ["gemini-proxy", { "toasts": false | "off" | "error" }]
  // 2. Environment variable: GEMINI_PROXY_TOASTS=off | false | error
  // Never call client.* here: OpenCode 1.18 loads plugins while loading its
  // config, so awaiting client.config.get() at init deadlocks startup.
  let rawSetting = options?.toasts
  if (rawSetting === undefined && process.env.GEMINI_PROXY_TOASTS !== undefined) {
    rawSetting = process.env.GEMINI_PROXY_TOASTS
  }

  const toastSetting = String(rawSetting ?? "info").toLowerCase()
  const minSev = MIN_SEVERITY[toastSetting] ?? 1

  const shouldToast = (type) => {
    if (minSev >= 99) return false
    if (type === "request_done" && !TOAST_DONE) return false
    const typeSev = SEVERITY[type] ?? 1
    return typeSev >= minSev
  }

  const log = (level, message, extra) => {
    try {
      const payload = { service: "gemini-proxy-plugin", level, message, extra }
      client.app?.log?.({ ...payload, body: payload })
    } catch {}
  }
  const toast = (message, variant, duration) => {
    try {
      const payload = { message, variant, duration }
      client.tui?.showToast?.({ ...payload, body: payload })
    } catch (err) {
      log("debug", "showToast failed", { err: String(err) })
    }
  }
  const recent = new Map() // msg -> last shown ts; 10s dedupe window
  const dedupedToast = (message, variant, duration) => {
    const now = Date.now()
    if (now - (recent.get(message) || 0) < 10000) return
    recent.set(message, now)
    toast(message, variant, duration)
  }
  let lastUnreachableToast = 0

  async function connect(sinceMs, backoffMs) {
    try {
      const res = await fetch(`${BASE_URL}/api/events?since=${sinceMs}`)
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
      log("debug", "events stream error, reconnecting", { err: String(err) })
    }
    await new Promise((r) => setTimeout(r, backoffMs))
    connect(Date.now(), Math.min(backoffMs * 2, 30000))
  }
  connect(Date.now(), 2000)

  return {
    event: async ({ event }) => {
      try {
        if (event?.type !== "session.idle") return
        const res = await fetch(`${BASE_URL}/health`).catch(() => null)
        if (!res || !res.ok) {
          const now = Date.now()
          if (now - lastUnreachableToast > 5 * 60000) {
            lastUnreachableToast = now
            if (shouldToast("request_failed")) {
              toast(`Gemini proxy not running at ${BASE_URL}. Start it with: npm start (or npx opencode-gemini-proxy)`, "error")
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
    },
  }
}

export const Plugin = GeminiProxyPlugin
export default GeminiProxyPlugin
