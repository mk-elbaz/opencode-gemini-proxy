# OpenCode Gemini Failover Proxy

An ultra-lightweight, **zero-dependency** OpenAI-compatible proxy designed specifically for **OpenCode** and **Google Gemini AI Studio** free tier.

It solves the primary pain point of free-tier API usage: **rate limits (HTTP 429)** — both per-minute (RPM) and per-day (RPD) — and upstream hangs, letting you multiply your effective capacity across multiple keys and models and keep coding without interruptions.

![OpenCode Gemini Failover Proxy Architecture](./assets/architecture.png)

---

## Features

- **Multi-Model Fallback Cascade**: Automatically rotates through Google's Gemini flash models when rate limits are hit:
  `gemini-3.8-flash` ➔ `gemini-3.7-flash` ➔ `gemini-3.6-flash` ➔ `gemini-3.5-flash` ➔ `gemini-3.5-flash-lite` ➔ `gemini-3.1-flash-lite`.
- **Multi-Key Round-Robin Pooling**: Load-balances requests across multiple Google API keys to multiply your quota pool.
- **Live Web Dashboard**: `http://localhost:8085` — add/remove API keys, enable/disable models, and add custom model IDs (e.g. a pro-tier model your plan grants access to) without touching `.env` by hand, plus live per-model/per-key stats.
- **Daily Quota Tracking**: Tracks requests sent per model per key, and detects when a 429 is a daily (RPD) limit vs. a short per-minute one — parking that model/key until Google's real reset (midnight **Pacific Time**, not UTC) instead of retrying uselessly for hours.
- **Thought-Signature Escape Hatch**: Automatically injects the documented `skip_thought_signature_validator` sentinel into replayed tool calls, so OpenCode subagents can execute tool loops without 400 errors.
- **True SSE Streaming Relay**: Line-buffered, verbatim event forwarding with chunk preservation, `[DONE]` synthesis, and mid-stream error detection.
- **Scope-Aware Exhaustion**: Distinguishes between per-model quotas (rotate to the next model on the same key) and key-wide worker limits (park the key too).
- **Wait-for-Quota (no aborted runs)**: When every model and key is rate-limited, the proxy *holds the request open* until quota returns rather than returning a 429 — a 429 makes OpenCode abort the whole agent run. The held request is kept alive with SSE keepalive comments, so a long run can sit through a daily-quota wall (up to 24h, configurable) and resume on its own once the limit resets. Configure with `PROXY_WAIT_FOR_QUOTA` / `PROXY_MAX_WAIT_HOURS`; set the former to `false` for the old fail-fast behavior.
- **Self-Healing Watchdog**: Optional `watchdog.vbs` (Windows) / `watchdog.sh` (macOS/Linux) auto-restarts the proxy within 3 seconds if it ever exits.
- **Observability**: Built-in `/health` and `/metrics` JSON endpoints.
- **Zero External Dependencies**: Pure Node.js built-in modules — no `npm install` required, no supply-chain surface.

---

## Requirements

- **Node.js 18+** (any OS). Check with `node --version`.
- A free **Google AI Studio API key** — get one at **[aistudio.google.com/apikey](https://aistudio.google.com/apikey)** (sign in with a Google account, click "Create API key"). You can add more than one key later from the dashboard to multiply your quota.

---

## Installation

### 1. Get the code

```bash
git clone https://github.com/mk-elbaz/opencode-gemini-proxy.git
cd opencode-gemini-proxy
```

(Or just download/copy the folder — there's nothing to build or `npm install`.)

### 2. Configure your API key

Copy the example env file and paste in your key:

**Windows (PowerShell):**
```powershell
Copy-Item .env.example .env
notepad .env
```

**macOS / Linux:**
```bash
cp .env.example .env
nano .env
```

```env
PROXY_PORT=8085
GOOGLE_API_KEY_1=your_api_key_here
```

You can add more keys later (`GOOGLE_API_KEY_2`, `GOOGLE_API_KEY_3`, …) either by editing `.env` directly or from the dashboard's "Add key" box — both work, the dashboard just saves you the restart.

### 3. Run the proxy

**Windows:**
```powershell
npm start
```
or double-click `start.bat` (shows a console window with live logs).

**macOS / Linux:**
```bash
npm start
```

Either way, you should see:
```
[Passthrough Failover Proxy] listening on http://localhost:8085
```

### 4. (Optional) Auto-restart on crash / at login

The proxy is a single long-running process; if you want it to survive a crash or reboot without you noticing:

- **Windows**: copy `watchdog.vbs` into your Startup folder (press <kbd>Win</kbd>+<kbd>R</kbd>, type `shell:startup`, Enter) to auto-start at login, or just double-click it to start it hidden right now.
- **macOS / Linux**: run `./watchdog.sh` in the background (`nohup ./watchdog.sh >/dev/null 2>&1 &`), or wrap it in a `systemd --user` unit / `launchd` plist for auto-start at login — see the comments at the top of `watchdog.sh` for exact commands.

### 5. Open the dashboard

Visit **http://localhost:8085** — this is your control panel: live request/latency stats, which models are in rotation (checkboxes), and your API keys (add/remove, see daily usage and cooldowns per key).

### 6. Configure OpenCode

Edit your OpenCode config (`~/.config/opencode/opencode.json` on macOS/Linux, `%USERPROFILE%\.config\opencode\opencode.json` on Windows — same relative path everywhere) and add the proxy as an OpenAI-compatible provider:

```json
{
  "provider": {
    "google": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "Google Auto-Failover Proxy (Local)",
      "options": {
        "baseURL": "http://localhost:8085/v1"
      },
      "models": {
        "gemini-3.8-flash": { "name": "Gemini 3.8 Flash (Auto-Failover)", "multimodal": true, "reasoning": true, "tool_call": true },
        "gemini-3.7-flash": { "name": "Gemini 3.7 Flash (Auto-Failover)", "multimodal": true, "reasoning": true, "tool_call": true },
        "gemini-3.6-flash": { "name": "Gemini 3.6 Flash (Auto-Failover)", "multimodal": true, "reasoning": true, "tool_call": true },
        "gemini-3.5-flash": { "name": "Gemini 3.5 Flash (Auto-Failover)", "multimodal": true, "reasoning": true, "tool_call": true },
        "gemini-3.5-flash-lite": { "name": "Gemini 3.5 Flash Lite (Auto-Failover)", "multimodal": true, "reasoning": true, "tool_call": true },
        "gemini-3.1-flash-lite": { "name": "Gemini 3.1 Flash Lite (Auto-Failover)", "multimodal": true, "reasoning": true, "tool_call": true }
      }
    }
  },
  "model": "google/gemini-3.8-flash",
  "small_model": "google/gemini-3.5-flash-lite"
}
```

No `apiKey` field is needed in this block — the proxy holds your real Google key(s) and OpenCode never sees them. This config works identically on Windows, macOS, and Linux; only the proxy's own run command (step 3) differs by OS.

Restart OpenCode (or start a new session) and pick `google/gemini-3.8-flash` (or any of the other five) as your model.

---

## Endpoints

- `GET /` — Web dashboard (stats, key management, model selection).
- `GET /v1/models` — Lists models currently in the failover rotation.
- `POST /v1/chat/completions` — OpenAI-compatible chat completion endpoint with automatic failover.
- `GET /health` — Liveness check, active cooling states, key counts, in-flight request counters.
- `GET /metrics` — Detailed telemetry per model and per key (attempts, successes, cooldowns, daily usage, latencies).
- `GET /api/config` — Current keys (masked) and enabled/available models, for the dashboard.
- `POST /api/keys` `{ "key": "..." }` — Add an API key.
- `DELETE /api/keys/:id` — Remove an API key (at least one must remain).
- `PUT /api/models` `{ "models": ["gemini-3.8-flash", ...] }` — Set which models are in the failover rotation.
- `POST /api/models` `{ "model": "..." }` — Add a custom model ID to the catalog (e.g. a pro-tier model your plan grants access to).
- `DELETE /api/models/:id` — Remove a custom model from the catalog (built-in models can't be removed).

---

## Development

Run the self-check test suite (Node's built-in test runner, no extra dependencies):

```bash
npm test
```

---

## License

MIT
