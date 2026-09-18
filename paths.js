/**
 * paths.js — where the proxy's mutable state (.env, usage.json, proxy.log)
 * lives. Pure function, no fs/env access at import time, so test.js can
 * exercise every branch without touching the real filesystem.
 *
 * Precedence:
 *   1. PROXY_DATA_DIR env var, if set — explicit override (Docker, custom setup).
 *   2. A `.env` next to server.js — today's git-checkout behavior, unchanged.
 *   3. ~/.config/opencode-gemini-proxy — same convention OpenCode itself uses
 *      on every OS (including Windows), for the npm-installed/npx case where
 *      there's no repo checkout to write into.
 */
import path from 'node:path';

export function resolveDataDir({ env, hasLocalEnv, homedir, moduleDir }) {
  if (env.PROXY_DATA_DIR) return env.PROXY_DATA_DIR;
  if (hasLocalEnv) return moduleDir;
  return path.join(homedir, '.config', 'opencode-gemini-proxy');
}
