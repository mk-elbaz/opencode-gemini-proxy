# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Event bus and `GET /api/events` SSE stream, powering live dashboard events and the OpenCode TUI plugin.
- In-stream `PROXY_STATUS` status lines rendered in OpenCode's thinking block (or visible content, or disabled).
- OpenCode TUI plugin (`opencode-plugin/`) showing toast notifications for model switches, slow responses, quota waits, daily quota hits, key rejections, and failed requests.
- Honor Google's `retryDelay`/`Retry-After` on a 429 instead of guessing the cooldown duration.
- Learned per-model daily quota limits, with a pace forecast (ETA to hitting the limit) exposed in `/metrics`.
- Token accounting (prompt/completion/total) per key, per model, and cumulatively, including today's tokens.
- Key validation against Google when a key is added via `.env`, the CLI, or the dashboard.
- Request history and a log-tail panel (`GET /api/logs`) in the dashboard.
- `PROXY_HOST` / `PROXY_ADMIN_TOKEN` to allow binding beyond loopback, gating `/api/*` and `/metrics` behind a bearer token.
- Dashboard `?demo=1` synthetic-data mode, light theme, and `?theme=light|dark` override.
- CLI (`cli.js`, `npx opencode-gemini-proxy`) with `init` (interactive key/`.env`/`opencode.json` setup with backup, plus TUI plugin install) and `doctor` (environment/health report) commands.
- Configurable data directory (`paths.js`, `PROXY_DATA_DIR`) so npm/npx installs without a git checkout have a writable home for `.env`/`usage.json`/`proxy.log`.
- Docker support (`Dockerfile`, `docker-compose.yml`) with a persistent data volume.
- CI workflow testing Node 18/20/22 on Linux, Windows, and macOS.
- CONTRIBUTING.md and SECURITY.md.
- Dynamic model discovery (`discovery.js`, `POST /api/models/discover`, `PROXY_DISCOVERY_HOURS`, `PROXY_AUTO_ENABLE_NEW_MODELS`) — the proxy now notices new or retired Gemini models from Google's own model list instead of relying only on the hardcoded default catalog.
- Dashboard "Refresh from Google" button, `new`/`unlisted` model badges, and a `models_discovered` event feed entry for model-discovery results.
- Tokens-per-minute (TPM) 429s are now classified separately from RPD/RPM (`quotaDimension`), learned the same way as daily limits, and surfaced in `/metrics` (`tpm_limit`, `pooled_tpm_limit`, `tokens_last_minute`) and the dashboard.

### Changed

- Dashboard extracted from `server.js` into its own file (`dashboard/index.html`) and redesigned with a live routing-pipeline visualizer, KPI row, and per-panel layout.

### Fixed

- `GET /` matched only the bare path, so any query string (e.g. `?demo=1`) 404'd — found while adding `?demo=1`.
- Learned daily (RPD) limits were computed from the pooled request count across all keys instead of the tripping key's own count, so with N keys the learned limit was inflated ~N×; it's now learned per key and pooled (`pooled_daily_limit`) for display.

## [1.0.0] - 2025-09-12

### Added

- Zero-dependency OpenAI-compatible proxy for Google Gemini AI Studio, built
  for OpenCode.
- Multi-model fallback cascade across the Gemini flash model lineup.
- Multi-key round-robin pooling to multiply quota across API keys.
- Live web dashboard with a real-time routing pipeline/fallback visualizer,
  key management (add/remove), and model enable/disable controls.
- Daily (RPD) vs short (RPM) quota detection, with per-key/per-model
  cooldown parking until Google's Pacific-time daily reset.
- Thought-signature escape hatch for OpenCode subagent tool loops.
- True SSE streaming relay with `[DONE]` synthesis and mid-stream error
  detection.
- Scope-aware exhaustion handling: per-model quotas rotate models on the
  same key; key-wide limits park the whole key.
- Wait-for-quota mode: holds requests open (SSE keepalive) instead of
  returning 429 when every model/key is rate-limited, configurable via
  `PROXY_WAIT_FOR_QUOTA` / `PROXY_MAX_WAIT_HOURS`.
- Self-healing watchdog scripts for Windows (`watchdog.vbs`) and
  macOS/Linux (`watchdog.sh`).
- `/health` and `/metrics` observability endpoints.
- Persistence of daily metrics, totals, and per-model/per-key stats across
  service restarts.
- Handling of upstream 403 by cooling the model and rotating instead of
  failing fast.
- Fix preventing usage metric inheritance when adding/removing API keys.
