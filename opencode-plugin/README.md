# opencode-gemini-proxy TUI plugin

Shows native OpenCode TUI toasts for what the failover proxy is doing:
model switches, slow responses, rate-limit waits, daily quota hits, key
rejections, and failed requests. Also checks proxy health when a session
goes idle and warns if the proxy is degraded, cooling down, or not running.

It's a passive observer — it reads the proxy's `/api/events` SSE stream and
`/health` endpoint over HTTP. It does not change routing or request
behavior.

## Install

### OpenCode V2
`npx opencode-gemini-proxy init` sets this up automatically. To install by hand:
1. Create directory `~/.config/opencode/plugins/gemini-proxy/`
2. Copy `gemini-proxy.js` to `~/.config/opencode/plugins/gemini-proxy/tui.js`
3. Add `package.json` there containing:
   ```json
   { "type": "module", "exports": { "./tui": "./tui.js" }, "oc-plugin": ["tui"] }
   ```
4. Register it in `~/.config/opencode/cli.json`:
   ```json
   {
     "plugins": [
       { "package": "./plugins/gemini-proxy", "options": { "toasts": "info" } }
     ]
   }
   ```
*(Note: OpenCode V2 requires a package directory with a `./tui` export for CLI toasts; a bare `.js` in `cli.json` is skipped.)*

### OpenCode V1 (1.18.x)
Copy `gemini-proxy.js` into an OpenCode plugin directory:
- Global: `~/.config/opencode/plugins/` (Windows: `%USERPROFILE%\.config\opencode\plugins\`)
- Per-project: `.opencode/plugins/` in your project root

V1 loads `.js`/`.ts` files in these directories automatically on startup — no config entry needed.

## Env vars

- `GEMINI_PROXY_URL` — base URL of the proxy. Default `http://127.0.0.1:8085`.
- `GEMINI_PROXY_TOAST_DONE` — set to `1` to also toast on every successful
  request (`request_done`). Off by default since it's noisy.

## Relation to `PROXY_STATUS`

The proxy's in-stream `PROXY_STATUS=reasoning` status line works whether or
not this plugin is installed — it's emitted directly in the model response
stream. This plugin is additive: it adds TUI toast notifications on top,
sourced from the proxy's separate `/api/events` stream.
