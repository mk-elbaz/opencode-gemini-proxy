# Contributing

## Run it

```bash
npm start
```

## Test it

```bash
node test.js
```

Node's built-in test runner, no framework. Add new cases to `test.js` as
plain `test('description', () => { ... assert.equal(...) })` blocks next to
the ones they resemble — most cover `quota.js` (quota classification) or
`compat.js` (OpenAI-spec fixes).

## Zero-dependency rule

This project has no npm dependencies and intends to keep it that way — pure
Node.js built-ins only. PRs that add a dependency (including dev
dependencies) will be declined. If the standard library can do it, use the
standard library.

## Where things live

- `server.js` — the main proxy HTTP gateway (failover, rate shaping, SSE relay, event bus).
- `cli.js` — command-line interface (`opencode-gemini-proxy init`, `doctor`, `start`).
- `dashboard/` — standalone web interface (`index.html`) with live routing visualizer.
- `discovery.js` — automatic Gemini model catalog discovery and diffing via Google API.
- `paths.js` — cross-platform data directory and configuration resolution.
- `status.js` — reasoning status line striping and event ring buffer helpers.
- `compat.js` — fixes for places Google's OpenAI-compat endpoint deviates from the OpenAI spec (tool call indices, `finish_reason` correction).
- `quota.js` — classifies 429 errors (per-model vs per-key, daily vs RPM) and extracts retry delays.
- `opencode-plugin/` — OpenCode TUI integration plugin for desktop toasts and status alerts.

## Commit messages

Conventional commits, as seen in `git log --oneline`: `feat: ...`,
`fix: ...`, short and in the imperative mood.
