/**
 * server.js — Google AI Studio OpenAI-compatible passthrough gateway
 * with auto-failover, rate shaping, and thought-signature backstop.
 *
 * Design (per architecture review):
 * - Verbatim passthrough: NO message rewriting, NO history trimming, NO role
 *   conversion, NO tool_calls stripping. Google's OpenAI-compat endpoint is
 *   mostly a genuine passthrough target, but it is NOT fully spec-compliant on
 *   the streaming path, so a few fields are filled in (see compat.js).
 * - Only transforms:
 *   1. model id mapping: "google/gemini-x" -> "gemini-x"
 *   2. inject `extra_content.google.thought_signature = "skip_thought_signature_validator"`
 *      into assistant tool_calls that lack a real signature (documented Google
 *      escape hatch; prevents the 400 "missing thought_signature" errors).
 *   3. add `tool_calls[].index`, which Google never sends.
 *   4. correct `finish_reason` "stop" -> "tool_calls" on a turn that carries
 *      tool calls. Google's streaming path gets this wrong (its non-streaming
 *      path gets it right), and clients use that field to tell "the model is
 *      done" from "a tool run is needed and this turn continues" — OpenCode
 *      renders a fresh agent/model/duration header per tool call without it.
 * - True SSE relay: line-buffered, events forwarded verbatim, `[DONE]`
 *   synthesized if upstream omits it, mid-stream errors detected and typed.
 * - Failover only BEFORE the first byte is sent to the client. Never rotate
 *   mid-stream.
 * - Rate shaping: per-model exponential cooldown + global concurrency cap +
 *   token bucket (~10 RPM conservative safety net — paces, never errors).
 *   Quotas are PER MODEL per key (Google documents RPM/TPM/RPD per model), so
 *   a 429 cools only that model and rotation continues on the SAME key. Keys
 *   cool only on key-wide evidence (worker-total limits, per-project quotaId)
 *   or auth failure. N keys ≈ N× pool.
 * - Timeouts: TTFT 15s, idle 30s per chunk, total 90s per attempt, 180s
 *   request budget. Long generations are allowed to run.
 * - Observability: GET /health (liveness + degraded state) and GET /metrics
 *   (per-model attempts/ok/cooldowns/errors/latency, token bucket, uptime).
 * - Logging rotates: proxy.log rolls to proxy.log.1 at 5MB (one backup kept).
 */

import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { quotaScope, isDailyQuota, msUntilPacificMidnight, retryDelayMs, learnedLimitFromTrip, quotaDimension } from './quota.js';
import { addToolCallIndices, correctFinishReason, patchStreamEvent } from './compat.js';
import { stripProxyStatusLines, pushRing } from './status.js';
import { resolveDataDir } from './paths.js';
import { filterChatModels, sortByVersionDesc, diffCatalog } from './discovery.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// The dashboard is code, not data — it always lives next to server.js
// regardless of where the mutable data dir (below) resolves to. It's read
// fresh on every GET / (see the route below) — it's ~40KB so the cost is
// nil, and it lets dashboard design iteration happen without restarting
// the proxy.
const DASHBOARD_PATH = path.join(__dirname, 'dashboard', 'index.html');

/* ------------------------------------------------------------------ */
/* Data directory — where .env / usage.json / proxy.log live. A git       */
/* checkout keeps today's behavior (next to server.js); an npm/npx install */
/* with no local .env falls back to the OS config dir OpenCode itself uses; */
/* PROXY_DATA_DIR always wins. See paths.js for the pure resolution logic. */
/* ------------------------------------------------------------------ */
const DATA_DIR = resolveDataDir({
  env: process.env,
  hasLocalEnv: fs.existsSync(path.join(__dirname, '.env')),
  homedir: os.homedir(),
  moduleDir: __dirname,
});
fs.mkdirSync(DATA_DIR, { recursive: true });

/* ------------------------------------------------------------------ */
/* Logging (size-bounded, crash-proof)                                */
/* ------------------------------------------------------------------ */
const LOG_FILE = path.join(DATA_DIR, 'proxy.log');
const LOG_BACKUP = path.join(DATA_DIR, 'proxy.log.1');
const LOG_MAX_BYTES = 5 * 1024 * 1024;

function rotateLogIfNeeded() {
  try {
    const st = fs.statSync(LOG_FILE);
    if (st.size <= LOG_MAX_BYTES) return;
    try { fs.rmSync(LOG_BACKUP, { force: true }); } catch {}
    fs.renameSync(LOG_FILE, LOG_BACKUP);
  } catch { /* fresh file or concurrent rotate — ignore */ }
}

function log(...args) {
  const msg = args.map(a => (typeof a === 'object' ? JSON.stringify(a) : String(a))).join(' ');
  const line = `[${new Date().toISOString()}] ${msg}\n`;
  console.log(line.trimEnd());
  try {
    rotateLogIfNeeded();
    fs.appendFileSync(LOG_FILE, line);
  } catch { /* logging must never crash the proxy */ }
}

/* ------------------------------------------------------------------ */
/* Event bus — powers GET /api/events (SSE feed for the dashboard/tools) */
/* In-memory only, last 200 events, oldest first. Every emitted event also  */
/* goes through log() once — call sites drop their own ad-hoc log() call.   */
/* ------------------------------------------------------------------ */
const EVENT_RING_MAX = 200;
const eventRing = [];
const eventSubscribers = new Set();
let reqCounter = 0;

function emitEvent(type, fields = {}) {
  const { msg, ...rest } = fields;
  const evt = { ts: Date.now(), type, msg, ...rest };
  pushRing(eventRing, evt, EVENT_RING_MAX);
  const data = `data: ${JSON.stringify(evt)}\n\n`;
  for (const sub of eventSubscribers) {
    if (!sub.writableEnded && !sub.destroyed) sub.write(data);
  }
  log(msg || type);
  return evt;
}

/* ------------------------------------------------------------------ */
/* .env loader                                                         */
/* ------------------------------------------------------------------ */
try {
  const envPath = path.join(DATA_DIR, '.env');
  if (fs.existsSync(envPath)) {
    for (const raw of fs.readFileSync(envPath, 'utf8').split('\n')) {
      const line = raw.trim();
      if (!line || line.startsWith('#') || line.startsWith('export ')) continue;
      const eq = line.indexOf('=');
      if (eq < 0) continue;
      const key = line.slice(0, eq).trim();
      let value = line.slice(eq + 1).trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      if (key && !process.env[key]) process.env[key] = value;
    }
  }
} catch (e) { log('env load error', e.message); }

/* ------------------------------------------------------------------ */
/* Config                                                              */
/* ------------------------------------------------------------------ */
const PORT = Number(process.env.PROXY_PORT || 8085);

/* Bind host + admin auth. Binding beyond loopback without a token would expose
 * the dashboard's key-management API to the network, so refuse to boot. */
const PROXY_HOST = process.env.PROXY_HOST || '127.0.0.1';
const LOOPBACK_HOSTS = ['127.0.0.1', 'localhost', '::1'];
const PROXY_ADMIN_TOKEN = process.env.PROXY_ADMIN_TOKEN || '';
if (!LOOPBACK_HOSTS.includes(PROXY_HOST) && !PROXY_ADMIN_TOKEN) {
  log(`FATAL: PROXY_HOST=${PROXY_HOST} is not loopback and PROXY_ADMIN_TOKEN is not set — refusing to bind on a non-local interface without auth`);
  process.exit(1);
}

// Skip Google key validation on add (e.g. offline dev, or testing).
const SKIP_KEY_CHECK = String(process.env.PROXY_SKIP_KEY_CHECK || '').toLowerCase() === 'true';

/* API keys — quota pools are per key/project, so N keys ≈ N× throughput.
 * .env accepts (priority order):
 *   GOOGLE_API_KEY_1, GOOGLE_API_KEY_2, ... (numbered, up to 10)
 *   GOOGLE_API_KEYS="k1,k2"                 (comma-separated)
 *   GOOGLE_API_KEY="k1"                     (single, legacy) */
const GOOGLE_KEYS = (() => {
  const single = (process.env.GOOGLE_API_KEY || '').trim();
  const numbered = [];
  for (let i = 1; i <= 10; i++) {
    const v = (process.env[`GOOGLE_API_KEY_${i}`] || '').trim();
    if (v) numbered.push(v);
  }
  // Numbered keys first, legacy single appended (deduplicated) — never drop a key.
  if (numbered.length) {
    if (single && !numbered.includes(single)) numbered.push(single);
    return numbered;
  }
  const csv = (process.env.GOOGLE_API_KEYS || '').split(',').map(s => s.trim()).filter(Boolean);
  if (csv.length) return csv;
  return single ? [single] : [];
})();

if (!GOOGLE_KEYS.length) {
  log('FATAL: no Google API key set in .env (GOOGLE_API_KEY / GOOGLE_API_KEY_1.. / GOOGLE_API_KEYS)');
  process.exit(1);
}

// Per-key rotation state. Full keys NEVER hit the log — only masked tails.
// Mutable (dashboard can add/remove keys at runtime) — kept as a `const` array,
// mutated via push/splice so every closure sharing this reference sees changes.
const keyState = GOOGLE_KEYS.map((key, i) => ({ id: i + 1, key, failures: 0, until: 0 }));
let nextKeyId = keyState.length ? Math.max(...keyState.map(k => k.id)) + 1 : 1;
let keyCursor = 0;
const maskKey = (k) => (k && k.length > 8 ? `...${k.slice(-4)}` : '...(short)');
const KEY_COOLDOWN_BASE_MS = 60000;
const KEY_COOLDOWN_MAX_MS = 300000;

// Built-in catalog — always offered regardless of plan.
const DEFAULT_MODELS = ['gemini-3.8-flash', 'gemini-3.7-flash', 'gemini-3.6-flash', 'gemini-3.5-flash', 'gemini-3.5-flash-lite', 'gemini-3.1-flash-lite'];
// Full catalog the dashboard can pick from: built-ins plus any custom model IDs
// a user with broader model access (e.g. a paid tier) has added — order here is
// the priority order used when a model isn't explicitly reordered via
// GOOGLE_ENABLED_MODELS. Mutable: addCustomModel/removeCustomModel push/splice it.
let ALL_MODELS = [...DEFAULT_MODELS, ...(process.env.GOOGLE_CUSTOM_MODELS || '').split(',').map(s => s.trim()).filter(m => m && !DEFAULT_MODELS.includes(m))];
// The active failover chain (subset/order of ALL_MODELS) — mutable, dashboard-editable.
let GOOGLE_MODELS = (() => {
  const csv = (process.env.GOOGLE_ENABLED_MODELS || '').split(',').map(s => s.trim()).filter(Boolean);
  const filtered = csv.filter(m => ALL_MODELS.includes(m));
  return filtered.length ? filtered : [...ALL_MODELS];
})();
const GOOGLE_ENDPOINT = 'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions';
const GOOGLE_MODELS_LIST_ENDPOINT = 'https://generativelanguage.googleapis.com/v1beta/models';

/* Model discovery — catalog ids added automatically by discoverModels(),
 * tracked separately from user-typed custom ids (addCustomModel) so
 * removeCustomModel's "can't remove a built-in" logic and saveEnvConfig's
 * GOOGLE_CUSTOM_MODELS persistence only ever see models a person actually
 * typed in. Catalog ids Google's API stops listing land in unlistedModelIds
 * (never auto-disabled — see discoverModels below). Both persist in
 * usage.json (see saveUsage/loadUsage) so they survive a restart. */
let discoveredModelIds = new Set();
let unlistedModelIds = new Set();
let lastDiscoveryAt = null; // ms epoch, or null before the first run
const DISCOVERY_HOURS = Number(process.env.PROXY_DISCOVERY_HOURS ?? 6);
const AUTO_ENABLE_NEW_MODELS = String(process.env.PROXY_AUTO_ENABLE_NEW_MODELS || '').toLowerCase() === 'true';

/* ------------------------------------------------------------------ */
/* Daily usage tracking (per key+model — Google's RPD quota is scoped   */
/* per model per key, not per key as a whole). Google doesn't expose a  */
/* "remaining quota" API for AI Studio keys, so this is our own count of */
/* requests sent through this proxy, resetting at midnight PACIFIC TIME  */
/* — Google's documented RPD reset boundary, NOT UTC — not "since you   */
/* last checked".                                                       */
/* ------------------------------------------------------------------ */
const USAGE_FILE = path.join(DATA_DIR, 'usage.json');
const PACIFIC_DATE_FMT = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Los_Angeles', year: 'numeric', month: '2-digit', day: '2-digit' });
function todayPacific() { return PACIFIC_DATE_FMT.format(new Date()); }
function getKeyFingerprint(key) {
  if (!key) return 'unknown';
  return crypto.createHash('sha256').update(key).digest('hex').slice(0, 12);
}
let usage = { day: todayPacific(), counts: {} }; // counts[keyId][model] = n

function bumpDailyUsage(keyId, model) {
  if (usage.day !== todayPacific()) { usage = { day: todayPacific(), counts: {} }; }
  // Self-heal: an older proxy version stored counts[keyId] as a flat number.
  if (typeof usage.counts[keyId] !== 'object' || usage.counts[keyId] === null) usage.counts[keyId] = {};
  const perKey = usage.counts[keyId];
  perKey[model] = (perKey[model] || 0) + 1;
  saveUsage();
}
// Total requests today for one key, across all models.
function requestsTodayForKey(keyId) {
  if (usage.day !== todayPacific()) return 0;
  const perKey = usage.counts[keyId] || {};
  return Object.values(perKey).reduce((a, b) => a + b, 0);
}
// Total requests today for one model, across all keys — this is what
// actually tracks against a per-model RPD limit.
function requestsTodayForModel(model) {
  if (usage.day !== todayPacific()) return 0;
  return Object.values(usage.counts).reduce((sum, perKey) => sum + (perKey[model] || 0), 0);
}

// model -> request count it tripped an RPD 429 at last time, for ONE key (RPD
// is scoped per key/project, not pooled — see learnedLimitFromTrip in
// quota.js). Learned, not documented anywhere by Google — survives day
// rollover (unlike `usage.counts`) so /metrics can forecast against it even
// before today's first 429. /metrics multiplies by activeKeyCount() to show
// the pooled total.
let learnedDailyLimit = {};

// model -> tokens-per-minute count it tripped a TPM 429 at last time, for ONE
// key. Same shape/persistence as learnedDailyLimit.
let learnedTpm = {};

// Keys not parked as permanently invalid (dead/revoked) — the pool size for
// turning a per-key learned limit into a pooled one.
function activeKeyCount() {
  return keyState.filter(k => !k.invalid).length;
}

// Sibling to bumpDailyUsage/requestsTodayFor*, same counts[keyId][model] shape,
// for token usage instead of request counts.
function bumpDailyTokens(keyId, model, u) {
  if (usage.day !== todayPacific()) { usage = { day: todayPacific(), counts: {} }; }
  if (typeof usage.tokenCounts !== 'object' || usage.tokenCounts === null) usage.tokenCounts = {};
  if (typeof usage.tokenCounts[keyId] !== 'object' || usage.tokenCounts[keyId] === null) usage.tokenCounts[keyId] = {};
  const cur = usage.tokenCounts[keyId][model] || { prompt: 0, completion: 0, total: 0 };
  cur.prompt += u.prompt_tokens || 0;
  cur.completion += u.completion_tokens || 0;
  cur.total += u.total_tokens || 0;
  usage.tokenCounts[keyId][model] = cur;
  saveUsage();
}
function tokensTodayForKey(keyId) {
  if (usage.day !== todayPacific()) return 0;
  const perKey = (usage.tokenCounts || {})[keyId] || {};
  return Object.values(perKey).reduce((sum, t) => sum + (t.total || 0), 0);
}
function tokensTodayForModel(model) {
  if (usage.day !== todayPacific()) return 0;
  return Object.values(usage.tokenCounts || {}).reduce((sum, perKey) => sum + (perKey[model]?.total || 0), 0);
}

/* ------------------------------------------------------------------ */
/* Dashboard-driven config mutations — persisted back to .env           */
/* ------------------------------------------------------------------ */
function saveEnvConfig() {
  const envPath = path.join(DATA_DIR, '.env');
  const kept = [];
  try {
    if (fs.existsSync(envPath)) {
      for (const raw of fs.readFileSync(envPath, 'utf8').split('\n')) {
        const line = raw.trim();
        if (!line || line.startsWith('#')) continue;
        const key = line.slice(0, line.indexOf('=')).trim();
        if (/^GOOGLE_API_KEY(_\d+)?$/.test(key) || key === 'GOOGLE_API_KEYS' || key === 'GOOGLE_ENABLED_MODELS' || key === 'GOOGLE_CUSTOM_MODELS') continue;
        kept.push(line);
      }
    }
  } catch (e) { log('env read (for save) error', e.message); }
  keyState.forEach((k, i) => kept.push(`GOOGLE_API_KEY_${i + 1}=${k.key}`));
  kept.push(`GOOGLE_ENABLED_MODELS=${GOOGLE_MODELS.join(',')}`);
  // Discovered-but-not-user-typed ids stay out of .env — they're persisted in
  // usage.json instead (see saveUsage/loadUsage) and re-added by discoverModels
  // on the next run either way, so writing them here would just be noise.
  const customModels = ALL_MODELS.filter(m => !DEFAULT_MODELS.includes(m) && !discoveredModelIds.has(m));
  if (customModels.length) kept.push(`GOOGLE_CUSTOM_MODELS=${customModels.join(',')}`);
  // Write-then-rename: a crash mid-write leaves the old .env intact instead
  // of a truncated file that drops every key on next load.
  try {
    const tmpPath = envPath + '.tmp';
    fs.writeFileSync(tmpPath, kept.join('\n') + '\n');
    fs.renameSync(tmpPath, envPath);
  } catch (e) { log('env write error', e.message); }
}

// Verify a new key against Google before adding it. Never logs the key.
// Rejects only on a clear "this key is bad" answer (400/403 API_KEY_INVALID /
// PERMISSION_DENIED); any network failure or timeout is treated as "can't
// tell" and lets the key through anyway, flagged with a warning.
async function verifyGoogleKey(key) {
  if (SKIP_KEY_CHECK) return { ok: true, modelsAvailable: null };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(key)}&pageSize=1`, { signal: controller.signal });
    const text = await res.text();
    let parsed = null;
    try { parsed = JSON.parse(text); } catch { /* not JSON */ }
    if (res.status === 400 || res.status === 403) {
      const blob = `${parsed?.error?.status || ''} ${parsed?.error?.message || text}`;
      if (/API_KEY_INVALID|PERMISSION_DENIED/i.test(blob)) {
        return { ok: false, rejected: true, message: (parsed?.error?.message || text || 'key rejected').slice(0, 200) };
      }
    }
    return { ok: true, modelsAvailable: Array.isArray(parsed?.models) ? parsed.models.length : null };
  } catch {
    return { ok: true, offline: true };
  } finally {
    clearTimeout(timer);
  }
}

function addKey(rawKey) {
  const key = String(rawKey || '').trim();
  if (!key) throw new Error('key is required');
  if (keyState.some(k => k.key === key)) throw new Error('key already exists');
  const id = nextKeyId++;
  // Prevent state inheritance: clear any stale usage metrics for this ID
  if (usage.counts[id]) {
    delete usage.counts[id];
    try { fs.writeFileSync(USAGE_FILE, JSON.stringify(usage)); } catch (e) { log('usage write error', e.message); }
  }
  keyState.push({ id, key, failures: 0, until: 0 });
  saveEnvConfig();
  log(`key added: key#${id} (${maskKey(key)})`);
  // Fire-and-forget: a fresh key may see a different/wider model list.
  discoverModels('key_added').catch(e => log('model discovery crashed:', e.message));
}

function removeKey(id) {
  if (keyState.length <= 1) throw new Error('cannot remove the last key');
  const idx = keyState.findIndex(k => k.id === id);
  if (idx < 0) throw new Error('key not found');
  const [removed] = keyState.splice(idx, 1);
  // Clean up usage metrics for the deleted key so we don't leak stats if the ID gets reused later
  if (usage.counts[id]) {
    delete usage.counts[id];
    try { fs.writeFileSync(USAGE_FILE, JSON.stringify(usage)); } catch (e) { log('usage write error', e.message); }
  }
  saveEnvConfig();
  log(`key removed: key#${removed.id} (${maskKey(removed.key)})`);
}

function setEnabledModels(models) {
  if (!Array.isArray(models) || !models.length) throw new Error('at least one model is required');
  const filtered = models.filter(m => ALL_MODELS.includes(m));
  if (!filtered.length) throw new Error('no valid model ids in list');
  GOOGLE_MODELS = filtered;
  saveEnvConfig();
  log(`enabled models set: ${GOOGLE_MODELS.join(', ')}`);
}

// Model IDs are Google's own free-text identifiers (e.g. a pro-tier model a
// paid plan grants access to) — just guard against garbage/injection, don't
// maintain a matching catalog.
const MODEL_ID_RE = /^[a-zA-Z0-9](?:[a-zA-Z0-9._-]{0,63})$/;

function addCustomModel(rawId) {
  const id = String(rawId || '').trim();
  if (!id) throw new Error('model id is required');
  if (!MODEL_ID_RE.test(id)) throw new Error('invalid model id');
  if (ALL_MODELS.includes(id)) throw new Error('model already in the catalog');
  ALL_MODELS.push(id);
  saveEnvConfig();
  log(`custom model added to catalog: ${id}`);
}

function removeCustomModel(id) {
  if (DEFAULT_MODELS.includes(id)) throw new Error('cannot remove a built-in model');
  const idx = ALL_MODELS.indexOf(id);
  if (idx < 0) throw new Error('model not found');
  if (GOOGLE_MODELS.includes(id) && GOOGLE_MODELS.length <= 1) throw new Error('cannot remove the only enabled model');
  ALL_MODELS.splice(idx, 1);
  GOOGLE_MODELS = GOOGLE_MODELS.filter(m => m !== id);
  discoveredModelIds.delete(id);
  unlistedModelIds.delete(id);
  saveEnvConfig();
  log(`custom model removed from catalog: ${id}`);
}

/* ------------------------------------------------------------------ */
/* Model discovery — notices when Google ships or retires Gemini models */
/* instead of relying solely on the hardcoded DEFAULT_MODELS list.      */
/* ------------------------------------------------------------------ */

// Catalog ids we'd expect Google's own model list to mention: the built-ins
// plus anything a previous discovery run already added. Deliberately
// excludes hand-typed custom ids (e.g. a pro-tier model not visible to every
// key) so those are never flagged "unlisted" just because this key can't see them.
function discoverableCatalogIds() {
  return ALL_MODELS.filter(m => DEFAULT_MODELS.includes(m) || discoveredModelIds.has(m));
}

function pickDiscoveryKey() {
  const now = Date.now();
  return keyState.find(k => !(k.until > now)) || keyState[0];
}

async function discoverModels(reason) {
  const key = pickDiscoveryKey().key;
  let models = [];
  let pageToken = null;
  let page = 0;
  try {
    do {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 8000);
      let url = `${GOOGLE_MODELS_LIST_ENDPOINT}?key=${encodeURIComponent(key)}&pageSize=200`;
      if (pageToken) url += `&pageToken=${encodeURIComponent(pageToken)}`;
      let res;
      try {
        res = await fetch(url, { signal: controller.signal });
      } finally {
        clearTimeout(timer);
      }
      if (res.status === 400 || res.status === 403) {
        log(`model discovery (${reason}): Google rejected the request (${res.status}) — skipping`);
        return;
      }
      if (!res.ok) {
        log(`model discovery (${reason}): HTTP ${res.status} from Google — skipping`);
        return;
      }
      const json = await res.json().catch(() => null);
      if (!json) {
        log(`model discovery (${reason}): could not parse Google's response — skipping`);
        return;
      }
      models = models.concat(Array.isArray(json.models) ? json.models : []);
      pageToken = json.nextPageToken || null;
      page++;
    } while (pageToken && page < 3);
  } catch (e) {
    log(`model discovery (${reason}): request failed (${e.message}) — skipping`);
    return;
  }

  const discovered = filterChatModels(models);
  const discoveredSet = new Set(discovered);
  const diff = diffCatalog(discoverableCatalogIds(), discovered);
  const added = diff.added.filter(id => !ALL_MODELS.includes(id));
  const removed = diff.removed; // catalog ids Google no longer lists

  for (const id of added) {
    ALL_MODELS.push(id);
    discoveredModelIds.add(id);
  }
  for (const id of removed) unlistedModelIds.add(id);
  for (const id of discoveredSet) unlistedModelIds.delete(id); // reappeared

  let autoEnabled = [];
  if (added.length && AUTO_ENABLE_NEW_MODELS) {
    GOOGLE_MODELS = sortByVersionDesc([...GOOGLE_MODELS, ...added]);
    autoEnabled = added.slice();
    saveEnvConfig();
  }

  lastDiscoveryAt = Date.now();
  saveUsage();

  log(`model discovery (${reason}): +${added.length} -${removed.length}` +
    (autoEnabled.length ? `, auto-enabled ${autoEnabled.length}` : '') +
    (added.length || removed.length ? '' : ' (no changes)'));

  if (added.length || removed.length) {
    const msg = added.length
      ? `New Gemini models available: ${added.join(', ')}. Enable them in the dashboard at http://localhost:${PORT}`
      : `Google no longer lists ${removed.join(', ')}; it stays enabled but may start failing`;
    emitEvent('models_discovered', { added, removed, autoEnabled, reason, msg });
  }
  return { added, removed };
}

const SIGNATURE_SENTINEL = 'skip_thought_signature_validator';
const EXHAUSTION_RE = /resource[_ ]?exhausted|rate[_ ]?limit|quota|too hot|overloaded|worker local total request limit|(?:^|[^a-z0-9])(429|503)(?:$|[^0-9])/i;

// quotaScope/isDailyQuota/msUntilPacificMidnight live in quota.js (imported above)
// so they're testable without booting the whole proxy — see test.js.

// Dead-key fingerprint: Google answers 400 (not 401) for bad/revoked keys.
const AUTH_ERROR_RE = /pass a valid api key|api key not valid|api_key_invalid|unauthenticated|invalid api key|unauthorized|permission denied/i;
const DEAD_KEY_PARK_MS = 30 * 60 * 1000;

/* Timeouts (ms) */
const TTFT_MS = 15000;        // headers must arrive within this
const IDLE_MS = 30000;        // no upstream bytes for this long -> abort
const TOTAL_ATTEMPT_MS = 90000;
const REQUEST_BUDGET_MS = 180000;

/* Wait-for-quota: when every model and key is cooling, hold the request open
 * until quota actually returns instead of 429ing. A 429 makes OpenCode abort
 * the whole agent run; waiting lets a long run survive a daily-quota wall and
 * resume by itself once the limit resets (up to PROXY_MAX_WAIT_HOURS later).
 * A held request is kept alive with SSE keepalive comments — `:`-prefixed
 * lines that SSE parsers ignore — so neither the client's header timeout nor
 * its idle timeout fires during a multi-hour wait. Set PROXY_WAIT_FOR_QUOTA
 * to "false" to get the old fail-fast behaviour back. */
const WAIT_FOR_QUOTA = String(process.env.PROXY_WAIT_FOR_QUOTA ?? 'true').toLowerCase() !== 'false';
const MAX_WAIT_MS = Math.max(0, Number(process.env.PROXY_MAX_WAIT_HOURS ?? 24)) * 60 * 60 * 1000;
const KEEPALIVE_MS = 10000;

/* In-band status messages: on failover/slow/waiting events, the proxy writes
 * a short status line into the streaming response via reasoning_content (or
 * content), so OpenCode shows it in the dim "thinking" block with zero client
 * config. "off" disables. A normal first-try success never gets one — the
 * response stays byte-for-byte verbatim, as today. */
const PROXY_STATUS = ['reasoning', 'content', 'off'].includes((process.env.PROXY_STATUS || '').toLowerCase())
  ? process.env.PROXY_STATUS.toLowerCase() : 'reasoning';

/* Rate shaping */
const COOLDOWN_BASE_MS = 30000;
const COOLDOWN_MAX_MS = 300000;
const MIN_COOLDOWN_MS = 2000; // floor when honoring Google's own RetryInfo delay
const MIN_INTERVAL_MS = 2000;
const MAX_CONCURRENT_GOOGLE = 2;
const TOKEN_BUCKET_RATE_MS = 6000;   // 1 token / 6s => ~10 req/min PER KEY (pools are per key)
const TOKEN_BUCKET_MAX = 1;
// Effective refill scales with key count: N keys ≈ N×10 req/min.
const bucketRateMs = () => TOKEN_BUCKET_RATE_MS / keyState.length;

/* ------------------------------------------------------------------ */
/* Rate shaping state                                                  */
/* ------------------------------------------------------------------ */
const modelState = new Map(); // model -> { until, failures, lastUsedAt }
let googleInFlight = 0;
const googleGate = [];
let bucketTokens = TOKEN_BUCKET_MAX;
let bucketFilledAt = Date.now();

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/* ------------------------------------------------------------------ */
/* Metrics (powers GET /health and GET /metrics)                       */
/* ------------------------------------------------------------------ */
const STARTED_AT = Date.now();
const totals = { requests: 0, ok: 0, final429: 0, failFast400: 0, upstreamError: 0, synthesizedStream: 0 };
const modelMetrics = new Map(); // model -> { attempts, ok, cooldowns, attemptErrors, totalLatencyMs, lastOkAt, lastCooldownAt }
const keyMetrics = new Map();   // key idx -> { attempts, ok, cooldowns, attemptErrors, totalLatencyMs, lastOkAt, lastCooldownAt }

/* Per-minute request-rate history — a 60-slot ring, oldest first, not
 * persisted (restarts just start a fresh window). Powers /metrics `history`
 * and the naive forecast in item 2. */
const HISTORY_SLOTS = 60;
const history = [];
function historyBucket() {
  const now = Date.now();
  const minuteTs = now - (now % 60000);
  let cur = history[history.length - 1];
  if (!cur || cur.ts !== minuteTs) {
    cur = { ts: minuteTs, requests: 0, ok: 0, errors: 0, latency_ms_sum: 0 };
    history.push(cur);
    if (history.length > HISTORY_SLOTS) history.shift();
  }
  return cur;
}
function historyBumpRequest() { historyBucket().requests++; }
function historyBumpOk(latencyMs) { const b = historyBucket(); b.ok++; b.latency_ms_sum += latencyMs; }
function historyBumpError() { historyBucket().errors++; }

/* Per-key, per-model sliding 60s token window — powers tokens_last_minute in
 * /metrics and TPM-limit learning. In-memory only, same as `history`. */
const TOKEN_WINDOW_MS = 60000;
const tokenWindows = new Map(); // `${keyId}:${model}` -> [{ts, tokens}, ...] oldest first
function pruneTokenWindow(arr, now = Date.now()) {
  while (arr.length && now - arr[0].ts > TOKEN_WINDOW_MS) arr.shift();
  return arr;
}
function recordTokenWindow(keyId, model, tokens) {
  if (!tokens) return;
  const wKey = `${keyId}:${model}`;
  let arr = tokenWindows.get(wKey);
  if (!arr) { arr = []; tokenWindows.set(wKey, arr); }
  arr.push({ ts: Date.now(), tokens });
  pruneTokenWindow(arr);
}
function tokensLastMinute(keyId, model) {
  const arr = tokenWindows.get(`${keyId}:${model}`);
  return arr ? pruneTokenWindow(arr).reduce((sum, e) => sum + e.tokens, 0) : 0;
}
function tokensLastMinuteForModel(model) {
  return keyState.reduce((sum, k) => sum + tokensLastMinute(k.id, model), 0);
}
function tokensLastMinuteForKey(keyId) {
  let sum = 0;
  for (const [wKey, arr] of tokenWindows) {
    if (wKey.startsWith(`${keyId}:`)) sum += pruneTokenWindow(arr).reduce((s, e) => s + e.tokens, 0);
  }
  return sum;
}

function saveUsage() {
  if (usage.day !== todayPacific()) {
    usage = { day: todayPacific(), counts: {} };
    totals.requests = 0;
    totals.ok = 0;
    totals.final429 = 0;
    totals.failFast400 = 0;
    totals.upstreamError = 0;
    totals.synthesizedStream = 0;
    modelMetrics.clear();
    keyMetrics.clear();
  }
  usage.totals = { ...totals };

  const mmObj = {};
  for (const [model, s] of modelMetrics.entries()) {
    mmObj[model] = { ...s };
  }
  usage.modelMetrics = mmObj;

  const kmObj = {};
  for (const [idx, s] of keyMetrics.entries()) {
    const kObj = keyState[idx];
    if (kObj && kObj.key) {
      const fp = getKeyFingerprint(kObj.key);
      kmObj[fp] = { ...s };
    }
  }
  usage.keyMetrics = kmObj;
  usage.learnedDailyLimit = learnedDailyLimit;
  usage.learnedTpm = learnedTpm;
  usage.discoveredModels = [...discoveredModelIds];
  usage.unlistedModels = [...unlistedModelIds];
  usage.lastDiscoveryAt = lastDiscoveryAt;

  try {
    fs.writeFileSync(USAGE_FILE, JSON.stringify(usage, null, 2));
  } catch (e) {
    log('usage write error', e.message);
  }
}

function loadUsage() {
  try {
    if (fs.existsSync(USAGE_FILE)) {
      const raw = JSON.parse(fs.readFileSync(USAGE_FILE, 'utf8'));
      // Learned daily limits outlive a day rollover — read them regardless of
      // whether the rest of `raw` is stale.
      if (raw && typeof raw.learnedDailyLimit === 'object' && raw.learnedDailyLimit) {
        learnedDailyLimit = raw.learnedDailyLimit;
      }
      if (raw && typeof raw.learnedTpm === 'object' && raw.learnedTpm) {
        learnedTpm = raw.learnedTpm;
      }
      // Discovered/unlisted catalog state outlives a day rollover too — it's
      // not part of the daily usage counters, just piggybacking on the same file.
      if (raw && Array.isArray(raw.discoveredModels)) {
        discoveredModelIds = new Set(raw.discoveredModels);
        for (const id of discoveredModelIds) if (!ALL_MODELS.includes(id)) ALL_MODELS.push(id);
      }
      if (raw && Array.isArray(raw.unlistedModels)) unlistedModelIds = new Set(raw.unlistedModels);
      if (raw && typeof raw.lastDiscoveryAt === 'number') lastDiscoveryAt = raw.lastDiscoveryAt;
      if (raw && raw.day === todayPacific()) {
        usage = raw;
        if (raw.totals) Object.assign(totals, raw.totals);
        if (raw.modelMetrics) {
          for (const [model, s] of Object.entries(raw.modelMetrics)) {
            modelMetrics.set(model, { ...s });
          }
        }
        if (raw.keyMetrics) {
          for (let idx = 0; idx < keyState.length; idx++) {
            const kObj = keyState[idx];
            if (kObj && kObj.key) {
              const fp = getKeyFingerprint(kObj.key);
              if (raw.keyMetrics[fp]) {
                keyMetrics.set(idx, { ...raw.keyMetrics[fp] });
              }
            }
          }
        }
        return;
      }
    }
  } catch (e) {
    log('usage load error', e.message);
  }
  usage = { day: todayPacific(), counts: {} };
}

loadUsage();

function mm(model) {
  let s = modelMetrics.get(model);
  if (!s) {
    s = { attempts: 0, ok: 0, cooldowns: 0, attemptErrors: 0, totalLatencyMs: 0, lastOkAt: 0, lastCooldownAt: 0 };
    modelMetrics.set(model, s);
  }
  return s;
}

function recordOk(model, attemptStart) {
  const s = mm(model);
  s.ok++;
  s.totalLatencyMs += Date.now() - attemptStart;
  s.lastOkAt = Date.now();
  totals.ok++;
  saveUsage();
}

function km(idx) {
  let s = keyMetrics.get(idx);
  if (!s) {
    s = { attempts: 0, ok: 0, cooldowns: 0, attemptErrors: 0, totalLatencyMs: 0, lastOkAt: 0, lastCooldownAt: 0 };
    keyMetrics.set(idx, s);
  }
  return s;
}

function recordKeyOk(idx, attemptStart) {
  const s = km(idx);
  s.ok++;
  s.totalLatencyMs += Date.now() - attemptStart;
  s.lastOkAt = Date.now();
  saveUsage();
}

// Records token usage (from either the non-streaming `usage` field or the
// streaming peek in relaySse) into per-model/per-key lifetime metrics, the
// running totals, and today's per-key/per-model daily record.
function recordTokens(model, keyIdx, u) {
  if (!u) return;
  const p = u.prompt_tokens || 0, c = u.completion_tokens || 0, t = u.total_tokens || (p + c);
  const ms = mm(model);
  ms.promptTokens = (ms.promptTokens || 0) + p;
  ms.completionTokens = (ms.completionTokens || 0) + c;
  ms.totalTokens = (ms.totalTokens || 0) + t;
  const ks = km(keyIdx);
  ks.promptTokens = (ks.promptTokens || 0) + p;
  ks.completionTokens = (ks.completionTokens || 0) + c;
  ks.totalTokens = (ks.totalTokens || 0) + t;
  totals.promptTokens = (totals.promptTokens || 0) + p;
  totals.completionTokens = (totals.completionTokens || 0) + c;
  totals.totalTokens = (totals.totalTokens || 0) + t;
  bumpDailyTokens(keyState[keyIdx].id, model, { prompt_tokens: p, completion_tokens: c, total_tokens: t });
  recordTokenWindow(keyState[keyIdx].id, model, t);
  saveUsage();
}

function markExhausted(model, daily = false, reqId = null, delayMs = null, keyId = null) {
  const s = modelState.get(model) || { until: 0, failures: 0, lastUsedAt: 0 };
  s.failures += 1;
  if (daily) {
    s.until = Date.now() + msUntilPacificMidnight();
    // Record the request count it tripped at, for the KEY that tripped it —
    // Google's RPD quota is per key (per project), not pooled, and Google
    // never publishes the number for AI Studio keys, so this is the only way
    // to learn one. See learnedLimitFromTrip in quota.js.
    if (keyId != null) {
      const count = learnedLimitFromTrip(usage.counts[keyId], model);
      if (count != null) learnedDailyLimit[model] = count;
    }
  } else if (delayMs != null) {
    // Honor Google's exact RetryInfo/Retry-After delay over our own guess,
    // still clamped to the same floor/ceiling as the exponential cooldown.
    s.until = Date.now() + Math.min(Math.max(delayMs + 1000, MIN_COOLDOWN_MS), COOLDOWN_MAX_MS);
  } else {
    s.until = Date.now() + Math.min(COOLDOWN_BASE_MS * 2 ** (s.failures - 1), COOLDOWN_MAX_MS);
  }
  s.dailyLimited = daily;
  modelState.set(model, s);
  const m = mm(model);
  m.cooldowns++;
  m.lastCooldownAt = Date.now();
  saveUsage();
  const resetsInS = Math.round((s.until - Date.now()) / 1000);
  const msg = `cooldown ${model} for ${resetsInS}s` +
    (daily ? ' (daily quota — parked until Pacific midnight)' : ` (failure #${s.failures})`);
  if (daily) emitEvent('quota_daily', { reqId, model, resetsInS, msg });
  else log(msg);
}

function markHealthy(model) {
  const s = modelState.get(model);
  if (s) { s.failures = 0; s.until = 0; s.dailyLimited = false; }
}

function pickModel() {
  const now = Date.now();
  for (const m of GOOGLE_MODELS) {
    const s = modelState.get(m);
    const cooling = s && s.until > now;
    const tooSoon = s && now - (s.lastUsedAt || 0) < MIN_INTERVAL_MS;
    if (!cooling && !tooSoon) {
      modelState.set(m, { ...(s || { failures: 0, until: 0 }), lastUsedAt: now });
      return m;
    }
  }
  return null;
}

function soonestCooldownMs() {
  const now = Date.now();
  return Math.min(...GOOGLE_MODELS.map(m => Math.max(0, (modelState.get(m)?.until || 0) - now)));
}

function acquireGoogle() {
  return new Promise(resolve => {
    if (googleInFlight < MAX_CONCURRENT_GOOGLE) { googleInFlight++; resolve(); }
    else googleGate.push(resolve);
  });
}
function releaseGoogle() {
  googleInFlight--;
  const next = googleGate.shift();
  if (next) { googleInFlight++; next(); }
}

function takeToken() {
  const now = Date.now();
  const rate = bucketRateMs();
  bucketTokens = Math.min(TOKEN_BUCKET_MAX, bucketTokens + (now - bucketFilledAt) / rate);
  bucketFilledAt = now;
  if (bucketTokens >= 1) { bucketTokens -= 1; return true; }
  return false;
}

/* Round-robin across healthy keys. Returns key index or -1 if all cooling. */
function pickKey() {
  const now = Date.now();
  for (let n = 0; n < keyState.length; n++) {
    const idx = (keyCursor + n) % keyState.length;
    if (!(keyState[idx].until > now)) {
      keyCursor = (idx + 1) % keyState.length;
      return idx;
    }
  }
  return -1;
}

function soonestKeyCooldownMs() {
  const now = Date.now();
  return Math.min(...keyState.map(k => Math.max(0, k.until - now)));
}

function markKeyExhausted(idx, daily = false, reqId = null, delayMs = null) {
  const k = keyState[idx];
  k.failures += 1;
  if (daily) {
    k.until = Date.now() + msUntilPacificMidnight();
  } else if (delayMs != null) {
    k.until = Date.now() + Math.min(Math.max(delayMs + 1000, MIN_COOLDOWN_MS), KEY_COOLDOWN_MAX_MS);
  } else {
    k.until = Date.now() + Math.min(KEY_COOLDOWN_BASE_MS * 2 ** (k.failures - 1), KEY_COOLDOWN_MAX_MS);
  }
  k.dailyLimited = daily;
  const m = km(idx);
  m.cooldowns++;
  m.lastCooldownAt = Date.now();
  const resetsInS = Math.round((k.until - Date.now()) / 1000);
  const msg = `cooldown key#${k.id} (${maskKey(k.key)}) for ${resetsInS}s` +
    (daily ? ' (daily quota — parked until Pacific midnight)' : ` (failure #${k.failures})`);
  if (daily) emitEvent('quota_daily', { reqId, keyId: k.id, resetsInS, msg });
  else log(msg);
}

function markKeyHealthy(idx) {
  const k = keyState[idx];
  k.failures = 0;
  k.until = 0;
  k.dailyLimited = false;
  k.invalid = false;
}

/* Scope-aware exhaustion: always cool the model; cool the key ONLY on
 * key-wide evidence. This is what makes rotation across per-model quotas work.
 * Free-tier RPD (requests-per-day) is documented PER MODEL per key — that's
 * the common case — so a day-scoped 429 mostly parks just the model. A 429
 * that's ALSO key-wide (rare: worker-total / per_project evidence) parks the
 * key too. Either way, day-scoped exhaustion won't recover within a short
 * exponential cooldown, so it's parked until the actual Pacific-time daily reset. */
function coolOnExhaustion(model, keyIdx, parsed, errMsg, reqId, retryAfterHeader = null) {
  const daily = isDailyQuota(parsed, errMsg);
  const dimension = daily ? 'rpd' : quotaDimension(parsed, errMsg);
  const delayMs = daily ? null : retryDelayMs(parsed, retryAfterHeader);
  const keyId = keyState[keyIdx]?.id ?? null;
  markExhausted(model, daily, reqId, delayMs, keyId);
  if (dimension === 'tpm' && keyId != null) {
    // Same idea as learnedDailyLimit but for tokens-per-minute: record what
    // this key was actually pushing through in the last 60s when it tripped.
    const tpm = tokensLastMinute(keyId, model);
    if (tpm > 0) learnedTpm[model] = tpm;
  }
  if (quotaScope(parsed, errMsg) === 'key') {
    markKeyExhausted(keyIdx, daily, reqId, delayMs);
    log(`key-wide exhaustion — key#${keyState[keyIdx].id} cooling too`);
  }
  return { daily, dimension };
}

/* ------------------------------------------------------------------ */
/* Payload transforms (minimal)                                        */
/* ------------------------------------------------------------------ */

// "google/gemini-3.6-flash" -> "gemini-3.6-flash"
function normalizeModel(model) {
  if (!model) return GOOGLE_MODELS[0];
  return model.includes('/') ? model.slice(model.indexOf('/') + 1) : model;
}

// addToolCallIndices/correctFinishReason/patchStreamEvent live in compat.js
// (imported above) so they are testable without booting the proxy.

// Inject the documented sentinel into any assistant tool_call missing a
// thought_signature. Google accepts it instead of 400ing on replay.
function ensureThoughtSignatures(messages) {
  if (!Array.isArray(messages)) return messages;
  let changed = false;
  for (const msg of messages) {
    if (msg.role !== 'assistant') continue;
    // Strip our own "[gemini-proxy] ..." status lines back out of replayed
    // reasoning_content before it's re-sent as history — see status.js.
    if (typeof msg.reasoning_content === 'string') {
      const stripped = stripProxyStatusLines(msg.reasoning_content);
      if (stripped === null) delete msg.reasoning_content;
      else msg.reasoning_content = stripped;
    }
    if (!Array.isArray(msg.tool_calls)) continue;
    for (const tc of msg.tool_calls) {
      const sig = tc.extra_content?.google?.thought_signature;
      if (!sig) {
        tc.extra_content = { google: { thought_signature: SIGNATURE_SENTINEL } };
        changed = true;
      }
    }
  }
  if (changed) log('injected skip_thought_signature_validator into replayed tool_calls');
  return messages;
}

/* ------------------------------------------------------------------ */
/* SSE relay — bulletproof passthrough                                 */
/* ------------------------------------------------------------------ */

async function relaySse(upstreamRes, res, model, onExhausted, guard, onFirstByte, onUsage = () => {}) {
  const streamState = { nextIndex: 0, sawToolCall: false };
  let headersSent = false;
  const sendHeaders = () => {
    if (headersSent) return;
    headersSent = true;
    // Already open if the request was held on a keepalive stream while waiting
    // for quota — the headers are identical, so just keep writing to it.
    if (res.headersSent) return;
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
  };

  const safeWrite = (str) => {
    if (res.writableEnded || res.destroyed) return;
    if (!res.write(str)) res.once('drain', () => {});
  };

  let done = false;
  const terminate = () => {
    if (res.writableEnded || res.destroyed) return;
    if (!done) safeWrite('data: [DONE]\n\n');
    res.end();
  };

  // Inspect-only: detect true upstream exhaustion/error events.
  // CRITICAL: NEVER inspect delta.content (which is generated text by the model)!
  const inspectEvent = (payload) => {
    let evt = null;
    try { evt = JSON.parse(payload); } catch { return false; }
    if (!evt) return false;
    
    if (evt.error || evt.choices?.[0]?.finish_reason === 'error') {
      const errObj = evt.error || { message: 'Upstream stream error' };
      onExhausted?.(model, errObj);
      safeWrite(`data: ${JSON.stringify({ error: {
        message: errObj.message || 'Upstream stream terminated with an error.',
        type: 'upstream_error', code: 'UPSTREAM_ERROR' } })}\n\n`);
      safeWrite('data: [DONE]\n\n');
      res.end();
      return true;
    }
    return false;
  };

  // Inspect-only peek: does not touch/reorder the event, just reports token
  // usage if this event happens to carry it (Google sends a final usage-only
  // event on some streams). Never throws on malformed JSON.
  const peekUsage = (payload) => {
    try {
      const evt = JSON.parse(payload);
      if (evt?.usage && typeof evt.usage.total_tokens === 'number') onUsage(evt.usage);
    } catch { /* ignore */ }
  };

  const decoder = new TextDecoder();
  let buf = '';
  let dataField = null; // SSE: consecutive data: lines join with \n

  try {
    for await (const chunk of upstreamRes.body) {
      if (guard) guard.kickIdle();
      onFirstByte?.();
      sendHeaders();
      buf += decoder.decode(chunk, { stream: true });
      let nl;
      while ((nl = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, nl).replace(/\r$/, '');
        buf = buf.slice(nl + 1);
        if (line === '') {
          if (dataField !== null) {
            if (dataField === '[DONE]') { safeWrite('data: [DONE]\n\n'); done = true; }
            else {
              peekUsage(dataField);
              if (!inspectEvent(dataField)) safeWrite(`data: ${patchStreamEvent(dataField, streamState)}\n\n`);
            }
            dataField = null;
          }
          if (done) { terminate(); return; }
          continue;
        }
        if (line.startsWith('data:')) {
          const v = line.slice(5).replace(/^ /, '');
          dataField = dataField === null ? v : `${dataField}\n${v}`;
        }
      }
    }
    if (dataField !== null) {
      peekUsage(dataField);
      if (!inspectEvent(dataField)) safeWrite(`data: ${patchStreamEvent(dataField, streamState)}\n\n`);
    }
    sendHeaders();
    terminate();
  } catch (err) {
    log(`relay stream error: ${err.message}`);
    sendHeaders();
    if (!done) {
      safeWrite(`data: ${JSON.stringify({ error: { message: 'Stream interrupted: ' + err.message, type: 'stream_error', code: 'STREAM_ERROR' } })}\n\n`);
    }
    terminate();
  }
}

/* ------------------------------------------------------------------ */
/* Timeout guard                                                       */
/* ------------------------------------------------------------------ */

function createTimeoutGuard({ ttftMs = TTFT_MS, idleMs = IDLE_MS, totalMs = TOTAL_ATTEMPT_MS } = {}) {
  const controller = new AbortController();
  let idleTimer = null;
  let totalTimer = null;
  let ttftTimer = null;

  const clearAll = () => { clearTimeout(idleTimer); clearTimeout(totalTimer); clearTimeout(ttftTimer); };
  const kickIdle = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => controller.abort(), idleMs);
    if (idleTimer.unref) idleTimer.unref();
  };
  ttftTimer = setTimeout(() => controller.abort(), ttftMs);
  if (ttftTimer.unref) ttftTimer.unref();
  totalTimer = setTimeout(() => controller.abort(), totalMs);
  if (totalTimer.unref) totalTimer.unref();

  return { signal: controller.signal, abort: () => controller.abort(), clearAll, kickIdle,
           markHeaders: () => { clearTimeout(ttftTimer); kickIdle(); } };
}

/* ------------------------------------------------------------------ */
/* JSON body reader with cap                                           */
/* ------------------------------------------------------------------ */

function readBody(req, res, maxBytes = 10 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', c => {
      size += c.length;
      if (size > maxBytes) {
        res.writeHead(413, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'Payload too large', status: 413 } }));
        req.destroy();
        reject(new Error('payload too large'));
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function sendJson(res, status, obj, headers = {}) {
  if (res.headersSent || res.writableEnded) return;
  res.writeHead(status, { 'Content-Type': 'application/json', ...headers });
  res.end(JSON.stringify(obj));
}

function safeEndSse(res, message) {
  if (res.writableEnded || res.destroyed) return;
  res.write(`data: ${JSON.stringify({ error: { message, type: 'stream_error', code: 'STREAM_ERROR' } })}\n\n`);
  res.write('data: [DONE]\n\n');
  res.end();
}

/* ------------------------------------------------------------------ */
/* Upstream attempt (Google)                                           */
/* ------------------------------------------------------------------ */

async function attemptGoogle(cleanPayload, isStreaming, guard, apiKey) {
  const upstreamRes = await fetch(GOOGLE_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': isStreaming ? 'text/event-stream' : 'application/json',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: JSON.stringify(cleanPayload),
    signal: guard.signal,
  });
  guard.markHeaders();
  return upstreamRes;
}

/* ------------------------------------------------------------------ */
/* HTTP server                                                         */
/* ------------------------------------------------------------------ */

const server = http.createServer(async (req, res) => {
  const urlPathOnly = req.url.split('?')[0];
  if (req.method === 'GET' && (urlPathOnly === '/' || urlPathOnly === '/index.html')) {
    let html;
    try { html = fs.readFileSync(DASHBOARD_PATH, 'utf8'); }
    catch (e) {
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Dashboard file missing: dashboard/index.html');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  // Admin auth gate — one check before route dispatch. `/v1/*`, `/health` and
  // `GET /` (above) stay open for OpenCode clients; everything else (the
  // dashboard's /api/* control-plane and /metrics) requires the token once
  // one is configured. ?token= is accepted too since EventSource (used by
  // /api/events) can't set request headers.
  if (PROXY_ADMIN_TOKEN) {
    const urlPath = req.url.split('?')[0];
    const isOpen = urlPath === '/health' || urlPath.startsWith('/v1/');
    if (!isOpen) {
      const authHeader = req.headers['authorization'] || '';
      const headerToken = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
      const queryToken = new URL(req.url, 'http://internal').searchParams.get('token');
      if ((headerToken || queryToken) !== PROXY_ADMIN_TOKEN) {
        sendJson(res, 401, { error: { message: 'Unauthorized', status: 401 } });
        return;
      }
    }
  }

  // Dashboard control-panel API — keys and enabled models.
  if (req.url === '/api/config' && req.method === 'GET') {
    const now = Date.now();
    sendJson(res, 200, {
      allModels: ALL_MODELS,
      defaultModels: DEFAULT_MODELS,
      enabledModels: GOOGLE_MODELS,
      discoveredModels: [...discoveredModelIds],
      unlistedModels: [...unlistedModelIds],
      lastDiscoveryAt,
      autoEnableNewModels: AUTO_ENABLE_NEW_MODELS,
      keys: keyState.map(k => ({
        id: k.id, masked: maskKey(k.key), cooling: k.until > now,
        daily_limited: !!k.dailyLimited, requests_today: requestsTodayForKey(k.id),
        resets_in_s: k.dailyLimited && k.until > now ? Math.ceil((k.until - now) / 1000) : null,
      })),
    });
    return;
  }

  if (req.url === '/api/keys' && req.method === 'POST') {
    try {
      const body = JSON.parse((await readBody(req, res)).toString('utf8') || '{}');
      const key = String(body.key || '').trim();
      const check = await verifyGoogleKey(key);
      if (check.rejected) {
        sendJson(res, 400, { error: `Google rejected this key: ${check.message}` });
        return;
      }
      addKey(key);
      const resp = { ok: true, models_available: check.modelsAvailable ?? null };
      if (check.offline) resp.warning = 'Could not verify key (offline?)';
      sendJson(res, 200, resp);
    } catch (e) { sendJson(res, 400, { error: e.message }); }
    return;
  }

  if (req.url.startsWith('/api/keys/') && req.method === 'DELETE') {
    try {
      removeKey(Number(req.url.slice('/api/keys/'.length)));
      sendJson(res, 200, { ok: true });
    } catch (e) { sendJson(res, 400, { error: e.message }); }
    return;
  }

  if (req.url === '/api/models' && req.method === 'PUT') {
    try {
      const body = JSON.parse((await readBody(req, res)).toString('utf8') || '{}');
      setEnabledModels(body.models);
      sendJson(res, 200, { ok: true });
    } catch (e) { sendJson(res, 400, { error: e.message }); }
    return;
  }

  // Add a model ID not in the built-in catalog (e.g. a pro-tier model a paid
  // plan grants access to). It lands in the catalog unchecked; enabling it
  // still goes through PUT /api/models like any other model.
  if (req.url === '/api/models' && req.method === 'POST') {
    try {
      const body = JSON.parse((await readBody(req, res)).toString('utf8') || '{}');
      addCustomModel(body.model);
      sendJson(res, 200, { ok: true });
    } catch (e) { sendJson(res, 400, { error: e.message }); }
    return;
  }

  // Manual "refresh from Google" trigger — same discovery run as the startup/
  // scheduled ones, just on demand from the dashboard button.
  if (req.url === '/api/models/discover' && req.method === 'POST') {
    try {
      const result = (await discoverModels('manual')) || { added: [], removed: [] };
      sendJson(res, 200, {
        ok: true, added: result.added, removed: result.removed,
        catalog: {
          allModels: ALL_MODELS, enabledModels: GOOGLE_MODELS,
          discoveredModels: [...discoveredModelIds], unlistedModels: [...unlistedModelIds],
        },
      });
    } catch (e) { sendJson(res, 400, { error: e.message }); }
    return;
  }

  if (req.url.startsWith('/api/models/') && req.method === 'DELETE') {
    try {
      removeCustomModel(decodeURIComponent(req.url.slice('/api/models/'.length)));
      sendJson(res, 200, { ok: true });
    } catch (e) { sendJson(res, 400, { error: e.message }); }
    return;
  }

  // Live event feed: replays the ring buffer (optionally from ?since=<ms>),
  // then streams new events as they're emitted. See emitEvent() above.
  if (req.method === 'GET' && req.url.split('?')[0] === '/api/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders(); // don't wait for the first write — an empty ring buffer means none for up to 15s
    const since = Number(new URL(req.url, 'http://internal').searchParams.get('since')) || 0;
    for (const evt of eventRing) {
      if (evt.ts > since) res.write(`data: ${JSON.stringify(evt)}\n\n`);
    }
    eventSubscribers.add(res);
    const ping = setInterval(() => { if (!res.writableEnded && !res.destroyed) res.write(': ping\n\n'); }, 15000);
    if (ping.unref) ping.unref();
    req.on('close', () => { clearInterval(ping); eventSubscribers.delete(res); });
    return;
  }

  // Dashboard log-tail panel: last N lines of proxy.log as plain text, capped
  // at 1000 lines. Full keys never reach proxy.log (see log()/maskKey above),
  // but mask any AIza... substring anyway in case one lands in an upstream
  // error message that got logged verbatim.
  if (req.method === 'GET' && req.url.split('?')[0] === '/api/logs') {
    const n = Math.min(1000, Math.max(1, Number(new URL(req.url, 'http://internal').searchParams.get('n')) || 200));
    let text = '';
    try {
      const lines = fs.readFileSync(LOG_FILE, 'utf8').split('\n').filter(Boolean);
      text = lines.slice(-n).join('\n');
    } catch { /* no log file yet */ }
    text = text.replace(/AIza[0-9A-Za-z_-]{10,}/g, (m) => `...${m.slice(-4)}`);
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(text);
    return;
  }

  if (req.method === 'GET' && req.url === '/v1/models') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      object: 'list',
      data: GOOGLE_MODELS.map(m => ({ id: m, object: 'model', created: Date.now(), owned_by: 'google' })),
    }));
    return;
  }

  if (req.method === 'GET' && req.url === '/health') {
    const now = Date.now();
    const cooling = GOOGLE_MODELS
      .filter(m => { const s = modelState.get(m); return s && s.until > now; })
      .map(m => ({ model: m, retry_in_s: Math.ceil((modelState.get(m).until - now) / 1000) }));
    const keysCooling = keyState
      .filter(k => k.until > now)
      .map(k => ({ key: `key#${k.id} (${maskKey(k.key)})`, retry_in_s: Math.ceil((k.until - now) / 1000) }));
    sendJson(res, 200, {
      status: cooling.length >= GOOGLE_MODELS.length ? 'degraded' : 'ok',
      uptime_s: Math.floor((now - STARTED_AT) / 1000),
      models: GOOGLE_MODELS,
      cooling,
      keys: { count: keyState.length, cooling: keysCooling },
      in_flight: googleInFlight,
    });
    return;
  }

  if (req.method === 'GET' && req.url === '/metrics') {
    const now = Date.now();
    // Naive forecast: minutes until a model's daily count reaches its learned
    // limit, at the overall request rate seen over the non-empty history
    // slots (there's no per-model breakdown in `history` — see item 5).
    // ponytail: a single shared rate applied per model is a rough estimate,
    // not per-model throughput; upgrade if forecast accuracy matters.
    const activeSlots = history.filter(b => b.requests > 0);
    const ratePerMin = activeSlots.length ? activeSlots.reduce((sum, b) => sum + b.requests, 0) / activeSlots.length : 0;
    const activeKeys = activeKeyCount();
    const forecast = Object.entries(learnedDailyLimit).map(([model, dailyLimit]) => {
      const pooledLimit = dailyLimit * activeKeys;
      const requestsToday = requestsTodayForModel(model);
      const etaMin = ratePerMin > 0 && requestsToday < pooledLimit ? Math.round((pooledLimit - requestsToday) / ratePerMin) : null;
      return { model, requests_today: requestsToday, daily_limit: dailyLimit, pooled_daily_limit: pooledLimit, eta_min: etaMin };
    });
    sendJson(res, 200, {
      uptime_s: Math.floor((now - STARTED_AT) / 1000),
      totals: {
        ...totals,
        tokens: { prompt: totals.promptTokens || 0, completion: totals.completionTokens || 0, total: totals.totalTokens || 0 },
      },
      bucket: {
        tokens: Math.round(bucketTokens * 1000) / 1000,
        refill_per_min: Math.round(60000 / bucketRateMs()),
        in_flight: googleInFlight,
        max_concurrent: MAX_CONCURRENT_GOOGLE,
      },
      history: history.filter(b => b.requests > 0),
      forecast,
      keys: keyState.map((k, idx) => {
        const s = keyMetrics.get(idx) || {};
        const isCooling = k.until > now;
        return {
          key: `key#${k.id} (${maskKey(k.key)})`,
          attempts: s.attempts || 0,
          ok: s.ok || 0,
          cooldowns: s.cooldowns || 0,
          attempt_errors: s.attemptErrors || 0,
          avg_latency_ms: s.ok ? Math.round(s.totalLatencyMs / s.ok) : null,
          last_ok_ago_s: s.lastOkAt ? Math.round((now - s.lastOkAt) / 1000) : null,
          cooling: isCooling,
          retry_in_s: isCooling ? Math.ceil((k.until - now) / 1000) : 0,
          tokens: { prompt: s.promptTokens || 0, completion: s.completionTokens || 0, total: s.totalTokens || 0 },
          tokens_today: tokensTodayForKey(k.id),
          tokens_last_minute: tokensLastMinuteForKey(k.id),
        };
      }),
      models: GOOGLE_MODELS.map(m => {
        const s = modelMetrics.get(m) || {};
        const st = modelState.get(m) || {};
        const isCooling = !!(st.until && st.until > now);
        const dailyLimit = learnedDailyLimit[m] ?? null;
        const tpmLimit = learnedTpm[m] ?? null;
        return {
          model: m,
          attempts: s.attempts || 0,
          ok: s.ok || 0,
          cooldowns: s.cooldowns || 0,
          attempt_errors: s.attemptErrors || 0,
          avg_latency_ms: s.ok ? Math.round(s.totalLatencyMs / s.ok) : null,
          last_ok_ago_s: s.lastOkAt ? Math.round((now - s.lastOkAt) / 1000) : null,
          cooling: isCooling,
          retry_in_s: isCooling ? Math.ceil((st.until - now) / 1000) : 0,
          daily_limited: !!st.dailyLimited,
          resets_in_s: st.dailyLimited && isCooling ? Math.ceil((st.until - now) / 1000) : null,
          requests_today: requestsTodayForModel(m),
          daily_limit: dailyLimit,
          pooled_daily_limit: dailyLimit != null ? dailyLimit * activeKeys : null,
          tpm_limit: tpmLimit,
          pooled_tpm_limit: tpmLimit != null ? tpmLimit * activeKeys : null,
          tokens_last_minute: tokensLastMinuteForModel(m),
          tokens: { prompt: s.promptTokens || 0, completion: s.completionTokens || 0, total: s.totalTokens || 0 },
          tokens_today: tokensTodayForModel(m),
        };
      }),
    });
    return;
  }

  if (req.method !== 'POST' || req.url !== '/v1/chat/completions') {
    sendJson(res, 404, { error: { message: 'Not found', status: 404 } });
    return;
  }

  // Crash net — a rejected handler must never kill the process.
  try {
    const rawBody = await readBody(req, res);
    const rawPayload = JSON.parse(rawBody.toString('utf8'));
    const messages = ensureThoughtSignatures(rawPayload.messages || []);
    const requestedModel = normalizeModel(rawPayload.model);
    const isStreaming = rawPayload.stream === true;

    const cleanPayload = {
      model: requestedModel,
      messages,
      temperature: rawPayload.temperature,
      max_tokens: rawPayload.max_tokens,
      stream: isStreaming,
      stream_options: rawPayload.stream_options,
      tools: rawPayload.tools,
      tool_choice: rawPayload.tool_choice,
      reasoning_effort: rawPayload.reasoning_effort,
      top_p: rawPayload.top_p,
      response_format: rawPayload.response_format,
    };
    // Drop undefined keys
    for (const k of Object.keys(cleanPayload)) if (cleanPayload[k] === undefined) delete cleanPayload[k];

    const reqId = ++reqCounter;
    log(`request: model=${requestedModel} stream=${isStreaming} msgs=${messages.length} tools=${!!rawPayload.tools} ctx=${cleanPayload.messages.length}`);
    totals.requests++;
    historyBumpRequest();
    saveUsage();

    const requestDeadline = Date.now() + (WAIT_FOR_QUOTA ? Math.max(MAX_WAIT_MS, REQUEST_BUDGET_MS) : REQUEST_BUDGET_MS);

    // A held request must notice if the user gives up and disconnects, so we
    // don't keep a dead socket parked for hours.
    let clientGone = false;
    res.once('close', () => { if (!res.writableEnded) clientGone = true; });
    // True once real model bytes have reached the client — distinct from
    // res.headersSent, which also flips when openKeepAlive() opens the SSE
    // stream early to ride out a quota wait (no model output yet).
    let realDataSent = false;

    /* Open the SSE stream early so a long wait can be kept alive. The headers
     * are the same ones relaySse would send, so the real stream just continues
     * on this one once a model frees up. Only possible for streaming clients —
     * a non-streaming request can't receive bytes before its single response. */
    let keepAliveOpen = false;
    const openKeepAlive = () => {
      if (keepAliveOpen || !isStreaming || res.headersSent || res.writableEnded) return;
      res.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no',
      });
      keepAliveOpen = true;
    };

    // In-band status line for OpenCode's "thinking" block — see PROXY_STATUS
    // above. No-op once real model bytes have flowed (realDataSent), so a
    // plain successful request never gets one; opens the SSE stream early
    // (same headers relaySse would send) if it hasn't been opened yet.
    const sendStatus = (text) => {
      if (!isStreaming || PROXY_STATUS === 'off' || realDataSent) return;
      openKeepAlive();
      if (res.writableEnded || res.destroyed) return;
      const field = PROXY_STATUS === 'content' ? 'content' : 'reasoning_content';
      res.write(`data: ${JSON.stringify({
        id: 'proxy-status',
        object: 'chat.completion.chunk',
        created: Math.floor(Date.now() / 1000),
        model: requestedModel,
        choices: [{ index: 0, delta: { [field]: `[gemini-proxy] ${text}\n` }, finish_reason: null }],
      })}\n\n`);
    };

    // Sleep in keepalive-sized slices. Returns false if the client vanished.
    // `reasonKey` is a human phrase from the call site ('all models cooling' /
    // 'all API keys cooling') — sniffed for 'key' to label the waiting event.
    const holdForQuota = async (ms, reasonKey) => {
      // A short wait rides out on the unopened response: staying unopened keeps
      // a real status code + Retry-After available if this ends up failing.
      // Only a genuinely long hold is worth committing to a stream for.
      if (ms <= KEEPALIVE_MS) {
        log(`${reasonKey}: waiting ${ms}ms`);
        await sleep(ms);
        return !clientGone;
      }
      const until = Date.now() + ms;
      const waitingReason = reasonKey.includes('key') ? 'keys' : 'models';
      const anyDaily = GOOGLE_MODELS.some(m => modelState.get(m)?.dailyLimited) || keyState.some(k => k.dailyLimited);
      const emitWaiting = () => {
        const remainingMs = Math.max(0, until - Date.now());
        let msg;
        if (anyDaily) {
          const h = Math.floor(remainingMs / 3600000);
          const mnt = Math.round((remainingMs % 3600000) / 60000);
          msg = `Daily quota hit on all ${waitingReason}, holding until Pacific midnight (${h}h${mnt}m). Add a key at http://localhost:${PORT}`;
        } else {
          msg = `All ${waitingReason} rate-limited, holding request, next slot in ${Math.round(remainingMs / 1000)}s`;
        }
        emitEvent('waiting', { reqId, reason: waitingReason, waitMs: remainingMs, daily: anyDaily, resetsInS: Math.round(remainingMs / 1000), msg });
        sendStatus(msg);
      };
      emitWaiting();
      openKeepAlive();
      let lastEmit = Date.now();
      while (Date.now() < until) {
        if (clientGone || res.writableEnded || res.destroyed) {
          log('client disconnected while waiting for quota — abandoning request');
          return false;
        }
        await sleep(Math.min(KEEPALIVE_MS, until - Date.now()));
        // `:` lines are SSE comments — ignored by parsers, but they keep the
        // socket (and the client's idle timer) alive.
        if (keepAliveOpen && !res.writableEnded && !res.destroyed) res.write(': waiting-for-quota\n\n');
        if (Date.now() - lastEmit >= 5 * 60 * 1000) { emitWaiting(); lastEmit = Date.now(); }
      }
      return true;
    };

    let lastFailed = null; // { model, reason } — set on a failed attempt, consumed as a model_switch event once the next model is picked

    while (Date.now() < requestDeadline) {
      if (clientGone) return;
      // Token bucket (per-key pools => refill scales with key count)
      if (!takeToken()) {
        const waitMs = Math.ceil((1 - bucketTokens) * bucketRateMs());
        log(`rate shaping: waiting ${waitMs}ms for token bucket`);
        await sleep(Math.min(waitMs, 5000));
        if (Date.now() >= requestDeadline) break;
      }

      const model = pickModel();
      if (!model) {
        const soonest = soonestCooldownMs();
        const waitMs = Math.min(Math.max(soonest, MIN_INTERVAL_MS), Math.max(requestDeadline - Date.now(), 0));
        if (waitMs > 0 && (WAIT_FOR_QUOTA || soonest <= 5000)) {
          if (!(await holdForQuota(waitMs, 'all models cooling'))) return;
          continue;
        }
        log('all Google models cooling down -> returning 429');
        break;
      }

      if (lastFailed) {
        const phrase = {
          rate_limit: 'rate-limited (per-minute)',
          rate_limit_tpm: 'hit its tokens-per-minute limit', rate_limit_rpm: 'hit its requests-per-minute limit',
          daily: 'hit its daily quota',
          timeout: 'timed out', error: 'errored', forbidden: 'was forbidden (403)',
        }[lastFailed.reason] || 'failed';
        const switchMsg = `${lastFailed.model} ${phrase}, trying ${model}`;
        emitEvent('model_switch', { reqId, from: lastFailed.model, to: model, reason: lastFailed.reason, msg: switchMsg });
        sendStatus(switchMsg);
        lastFailed = null;
      }

      const keyIdx = pickKey();
      if (keyIdx < 0) {
        const soonest = soonestKeyCooldownMs();
        const waitMs = Math.min(Math.max(soonest, MIN_INTERVAL_MS), Math.max(requestDeadline - Date.now(), 0));
        if (waitMs > 0 && (WAIT_FOR_QUOTA || soonest <= 5000)) {
          if (!(await holdForQuota(waitMs, 'all API keys cooling'))) return;
          continue;
        }
        log('all Google API keys cooling down -> returning 429');
        break;
      }
      // Count attempts only when model AND key are both confirmed — otherwise
      // /metrics shows phantom attempts on models that were never actually tried.
      mm(model).attempts++;
      km(keyIdx).attempts++;
      bumpDailyUsage(keyState[keyIdx].id, model);
      const apiKey = keyState[keyIdx].key;

      await acquireGoogle();
      const guard = createTimeoutGuard();
      const abortOnClose = () => {
        try { guard.abort(); } catch { /* ignore */ }
      };
      res.once('close', abortOnClose);
      let slowTimer = null;

      try {
        const payload = { ...cleanPayload, model };
        const attemptStart = Date.now();
        slowTimer = setTimeout(() => {
          const slowMsg = `${model} is slow to respond (5s, key #${keyState[keyIdx].id})…`;
          emitEvent('slow_response', { reqId, model, keyId: keyState[keyIdx].id, waitedMs: 5000, msg: slowMsg });
          sendStatus(slowMsg);
        }, 5000);
        if (slowTimer.unref) slowTimer.unref();
        const upstreamRes = await attemptGoogle(payload, isStreaming, guard, apiKey);
        clearTimeout(slowTimer);

        const ct = (upstreamRes.headers.get('content-type') || '').toLowerCase();
        const isSse = ct.includes('text/event-stream');

        if (isSse) {
          // True streaming path — relay verbatim; detect errors per-event.
          await relaySse(upstreamRes, res, model,
            (m, errObj) => coolOnExhaustion(m, keyIdx, { error: errObj }, JSON.stringify(errObj || ''), reqId, upstreamRes.headers.get('retry-after')),
            guard, () => { realDataSent = true; },
            (usage) => recordTokens(model, keyIdx, usage));
          markHealthy(model);
          markKeyHealthy(keyIdx);
          recordOk(model, attemptStart);
          recordKeyOk(keyIdx, attemptStart);
          const latencyMs = Date.now() - attemptStart;
          historyBumpOk(latencyMs);
          const requestsToday = requestsTodayForKey(keyState[keyIdx].id);
          emitEvent('request_done', {
            reqId, model, keyId: keyState[keyIdx].id, latencyMs, requestsToday, stream: isStreaming,
            msg: `Answered by ${model} in ${(latencyMs / 1000).toFixed(1)}s via key #${keyState[keyIdx].id} (${requestsToday} requests today)`,
          });
          return;
        }

        // Non-SSE body — bounded read and inspect.
        const text = await upstreamRes.text();
        guard.kickIdle();

        let parsed = null;
        try { parsed = JSON.parse(text); } catch { /* not JSON */ }

        const errMsg = parsed?.error?.message || text || '';
        const exhausted =
          upstreamRes.status === 429 || upstreamRes.status === 503 ||
          EXHAUSTION_RE.test(errMsg) ||
          parsed?.error?.status === 'RESOURCE_EXHAUSTED' ||
          parsed?.error?.code === 429;

        if (exhausted) {
          const retryAfter = upstreamRes.headers.get('retry-after');
          const { daily, dimension } = coolOnExhaustion(model, keyIdx, parsed, errMsg, reqId, retryAfter);
          lastFailed = { model, reason: daily ? 'daily' : dimension === 'tpm' ? 'rate_limit_tpm' : dimension === 'rpm' ? 'rate_limit_rpm' : 'rate_limit' };
          log(`exhausted on ${model} (${upstreamRes.status}): ${errMsg.slice(0, 200)} retry-after=${retryAfter}`);
          continue; // rotate with cooldown
        }

        if (upstreamRes.status === 400 || upstreamRes.status === 403) {
          if (AUTH_ERROR_RE.test(errMsg)) {
            // Dead/revoked key — park it long and rotate, don't fail the request.
            markKeyExhausted(keyIdx, false, reqId);
            keyState[keyIdx].until = Date.now() + DEAD_KEY_PARK_MS;
            keyState[keyIdx].invalid = true;
            const krMsg = `key#${keyState[keyIdx].id} rejected as invalid — parked 30m, rotating`;
            emitEvent('key_rejected', { reqId, keyId: keyState[keyIdx].id, msg: krMsg });
            sendStatus(krMsg);
            continue;
          }
          if (upstreamRes.status === 403) {
            // Permission denied for this model on this key (e.g. a custom/pro
            // model the key's project isn't entitled to) — cool the model and
            // rotate, don't kill the whole request over one model's access.
            markExhausted(model, false, reqId);
            lastFailed = { model, reason: 'forbidden' };
            log(`403 from ${model}: ${errMsg.slice(0, 300)} — rotating`);
            continue;
          }
          // Deterministic 400 payload error — fail fast, do not burn models.
          totals.failFast400++;
          saveUsage();
          historyBumpError();
          emitEvent('request_failed', { reqId, status: upstreamRes.status, msg: `400 from ${model}: ${errMsg.slice(0, 300)}` });
          // If we already opened a keepalive stream, a status code is no longer
          // available — report it in-stream instead of silently hanging.
          if (res.headersSent) safeEndSse(res, errMsg);
          else sendJson(res, 400, { error: { message: errMsg, status: 400 } });
          return;
        }

        if (!upstreamRes.ok || parsed?.error || !parsed?.choices?.length) {
          totals.upstreamError++;
          saveUsage();
          historyBumpError();
          emitEvent('request_failed', { reqId, status: upstreamRes.status || 502, msg: `upstream error ${upstreamRes.status} from ${model}: ${errMsg.slice(0, 300)}` });
          if (res.headersSent) safeEndSse(res, errMsg);
          else sendJson(res, upstreamRes.status || 502, { error: { message: errMsg, status: upstreamRes.status || 502 } });
          return;
        }

        markHealthy(model);
        markKeyHealthy(keyIdx);
        recordOk(model, attemptStart);
        recordKeyOk(keyIdx, attemptStart);

        if (isStreaming) {
          // Non-SSE JSON to a streaming client: synthesize a compliant single-chunk stream.
          if (!res.headersSent) res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache' });
          const msg = parsed.choices[0].message || {};
          addToolCallIndices(msg.tool_calls);
          const synthChoice = { finish_reason: parsed.choices[0].finish_reason || 'stop' };
          correctFinishReason(synthChoice, msg.tool_calls);
          res.write(`data: ${JSON.stringify({ id: parsed.id, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: { role: 'assistant', ...msg }, finish_reason: null }] })}\n\n`);
          res.write(`data: ${JSON.stringify({ id: parsed.id, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model, choices: [{ index: 0, delta: {}, finish_reason: synthChoice.finish_reason }] })}\n\n`);
          if (parsed.usage) {
            res.write(`data: ${JSON.stringify({ id: parsed.id, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model, choices: [], usage: parsed.usage })}\n\n`);
            recordTokens(model, keyIdx, parsed.usage);
          }
          res.write('data: [DONE]\n\n');
          res.end();
          totals.synthesizedStream++;
          saveUsage();
          {
            const latencyMs = Date.now() - attemptStart;
            historyBumpOk(latencyMs);
            const requestsToday = requestsTodayForKey(keyState[keyIdx].id);
            emitEvent('request_done', {
              reqId, model, keyId: keyState[keyIdx].id, latencyMs, requestsToday, stream: isStreaming,
              msg: `Answered by ${model} in ${(latencyMs / 1000).toFixed(1)}s via key #${keyState[keyIdx].id} (${requestsToday} requests today) [synthesized]`,
            });
          }
          return;
        }

        addToolCallIndices(parsed.choices[0].message?.tool_calls);
        correctFinishReason(parsed.choices[0], parsed.choices[0].message?.tool_calls);
        if (parsed.usage) recordTokens(model, keyIdx, parsed.usage);
        sendJson(res, 200, parsed);
        {
          const latencyMs = Date.now() - attemptStart;
          historyBumpOk(latencyMs);
          const requestsToday = requestsTodayForKey(keyState[keyIdx].id);
          emitEvent('request_done', {
            reqId, model, keyId: keyState[keyIdx].id, latencyMs, requestsToday, stream: isStreaming,
            msg: `Answered by ${model} in ${(latencyMs / 1000).toFixed(1)}s via key #${keyState[keyIdx].id} (${requestsToday} requests today)`,
          });
        }
        return;

      } catch (err) {
        const aborted = guard.signal.aborted;
        mm(model).attemptErrors++;
        km(keyIdx).attemptErrors++;
        saveUsage();
        log(`attempt ${model} failed: ${err.message} aborted=${aborted}`);

        if (aborted && realDataSent) {
          // Mid-stream abort after real model bytes sent — terminate cleanly, no rotation.
          safeEndSse(res, 'Upstream stalled mid-stream');
          return;
        }
        if (aborted && (err.name === 'AbortError' || err.message.includes('abort'))) {
          // Timeout before headers — per-model stall, rotate (key presumably fine).
          markExhausted(model, false, reqId);
          lastFailed = { model, reason: 'timeout' };
          continue;
        }
        // Network error — per-model cooldown, rotate (key presumably fine).
        markExhausted(model, false, reqId);
        lastFailed = { model, reason: 'error' };
        continue;
      } finally {
        clearTimeout(slowTimer);
        guard.clearAll();
        res.off('close', abortOnClose);
        releaseGoogle();
      }
    }

    /* ---- All Google models exhausted ---- */
    totals.final429++;
    saveUsage();
    historyBumpError();
    const nowDone = Date.now();
    const modelStatus = GOOGLE_MODELS.map(m => {
      const s = modelState.get(m) || {};
      return { model: m, retry_in_s: s.until > nowDone ? Math.ceil((s.until - nowDone) / 1000) : 0, daily_limited: !!s.dailyLimited };
    });
    const keyStatus = keyState.map(k => ({
      key: `key#${k.id}`, retry_in_s: k.until > nowDone ? Math.ceil((k.until - nowDone) / 1000) : 0, daily_limited: !!k.dailyLimited,
    }));
    const anyDaily = modelStatus.some(m => m.daily_limited) || keyStatus.some(k => k.daily_limited);
    // Truthful Retry-After: tell the client when relief actually arrives
    // (soonest model/key cooldown), so OpenCode's backoff waits the right time.
    // A daily-quota block won't clear in seconds, so don't clamp it down to 300s
    // and don't tell the client "try again in a few seconds" when it's really hours.
    const soonestS = Math.max(0, Math.ceil(Math.max(soonestCooldownMs(), soonestKeyCooldownMs()) / 1000));
    const retryAfterS = anyDaily ? soonestS : Math.min(300, Math.max(5, soonestS));
    const allKeysInvalid = keyState.every(k => k.invalid);
    const giveUpMessage = allKeysInvalid
      ? `Every configured Google API key was rejected as invalid (${keyState.length} key${keyState.length === 1 ? '' : 's'}). Check the key at https://aistudio.google.com/apikey or add a working one at http://localhost:${PORT}.`
      : anyDaily
      ? `All enabled Gemini models/keys are rate-limited, and at least one has hit its daily quota (resets at Pacific midnight, ~${Math.ceil(soonestS / 60)}m). Enable another model, add a key, or wait for the reset.`
      : 'All Gemini models are currently rate-limited (free-tier worker limit). Try again in a few seconds.';
    // Only reachable with wait-for-quota off, or after the full wait budget
    // elapsed. If a keepalive stream is already open there's no status code
    // left to send, so the failure goes in-stream.
    emitEvent('request_failed', { reqId, status: 429, msg: giveUpMessage });
    if (res.headersSent) safeEndSse(res, giveUpMessage);
    else sendJson(res, 429, {
      error: { message: giveUpMessage, status: 429, details: { models: modelStatus, keys: keyStatus } },
    }, { 'Retry-After': String(retryAfterS) });

  } catch (err) {
    log('handler error:', err.message);
    if (res.headersSent) safeEndSse(res, err.message);
    else sendJson(res, 500, { error: { message: err.message, status: 500 } });
  }
});

/* ------------------------------------------------------------------ */
/* Server hardening                                                    */
/* ------------------------------------------------------------------ */
server.keepAliveTimeout = 60000;
server.headersTimeout = 15000;
server.requestTimeout = 300000;
server.maxRequestsPerSocket = 0;

process.on('unhandledRejection', reason => {
  log('UNHANDLED REJECTION:', reason?.message || reason);
});

process.on('uncaughtException', err => {
  log('UNCAUGHT EXCEPTION:', err?.stack || err?.message || err);
  // exit so the startup watchdog can restart a clean instance
  process.exit(1);
});

server.on('error', err => {
  if (err.code === 'EADDRINUSE') {
    // another instance is already serving — wait before exit so watchdog doesn't spin-loop
    log('EADDRINUSE: port already in use (another instance running) — waiting before retry');
    setTimeout(() => process.exit(0), 30000);
    return;
  }
  log('SERVER ERROR:', err?.stack || err?.message || err);
  process.exit(1);
});

['SIGINT', 'SIGTERM', 'SIGHUP', 'beforeExit'].forEach(sig => {
  process.on(sig, () => {
    saveUsage();
    if (sig !== 'beforeExit') process.exit(0);
  });
});

server.listen(PORT, PROXY_HOST, () => {
  log(`[Passthrough Failover Proxy] data dir: ${DATA_DIR}`);
  log(`[Passthrough Failover Proxy] listening on http://${PROXY_HOST}:${PORT}`);
  log(`[Passthrough Failover Proxy] google models: ${GOOGLE_MODELS.join(', ')}`);
  log(`[Passthrough Failover Proxy] api keys: ${keyState.length} (${keyState.map(k => `key#${k.id} ${maskKey(k.key)}`).join(', ')})`);

  // Model discovery: once shortly after boot (non-blocking), then on a
  // schedule. PROXY_DISCOVERY_HOURS=0 disables the recurring run entirely.
  const startupTimer = setTimeout(() => {
    discoverModels('startup').catch(e => log('model discovery crashed:', e.message));
  }, 3000);
  if (startupTimer.unref) startupTimer.unref();
  if (DISCOVERY_HOURS > 0) {
    const discoveryInterval = setInterval(() => {
      discoverModels('scheduled').catch(e => log('model discovery crashed:', e.message));
    }, DISCOVERY_HOURS * 60 * 60 * 1000);
    if (discoveryInterval.unref) discoveryInterval.unref();
  }
});
