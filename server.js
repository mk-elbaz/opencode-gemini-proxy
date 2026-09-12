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
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { quotaScope, isDailyQuota, msUntilPacificMidnight } from './quota.js';
import { addToolCallIndices, correctFinishReason, patchStreamEvent } from './compat.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OpenCode Gemini Failover Proxy</title>
    <style>
        :root {
            --bg: #0a0e14; --bg-elevated: #11161f; --border: #1e2530;
            --text: #e6e9ef; --text-dim: #78828f; --text-faint: #4a5261;
            --ok: #3ddc84; --ok-dim: rgba(61, 220, 132, 0.12);
            --warn: #f0b352; --warn-dim: rgba(240, 179, 82, 0.12);
            --danger: #f26d6d; --danger-dim: rgba(242, 109, 109, 0.12);
            --accent: #6ea8fe; --accent-dim: rgba(110, 168, 254, 0.12);
            --mono: 'SF Mono', 'Cascadia Code', Consolas, monospace;
            --sans: -apple-system, 'Segoe UI', system-ui, sans-serif;
            --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.24), 0 1px 1px rgba(0, 0, 0, 0.16);
            --shadow-md: 0 8px 24px rgba(0, 0, 0, 0.28), 0 2px 6px rgba(0, 0, 0, 0.2);
            --ease: cubic-bezier(0.16, 1, 0.3, 1);
            color-scheme: dark;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        @media (prefers-reduced-motion: reduce) {
            *, *::before, *::after { animation-duration: 0.001ms !important; transition-duration: 0.001ms !important; }
        }
        body {
            background: var(--bg); color: var(--text); font-family: var(--sans);
            padding: 40px 24px 64px; line-height: 1.4;
            background-image: radial-gradient(circle at 15% 0%, rgba(110, 168, 254, 0.06), transparent 45%);
        }
        ::selection { background: var(--accent-dim); color: var(--text); }
        ::-webkit-scrollbar { width: 10px; height: 10px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 100px; border: 2px solid var(--bg); }
        ::-webkit-scrollbar-thumb:hover { background: var(--text-faint); }
        html { scrollbar-color: var(--border) transparent; }
        :focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; border-radius: 4px; }

        .container { max-width: 980px; margin: 0 auto; }
        header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 32px; flex-wrap: wrap; gap: 12px; }
        .title-group { display: flex; align-items: center; gap: 12px; }
        .logo { width: 34px; height: 34px; border-radius: 9px; display: flex; align-items: center; justify-content: center;
            background: linear-gradient(160deg, var(--accent-dim), transparent); border: 1px solid var(--border); flex-shrink: 0; }
        .logo svg { width: 17px; height: 17px; color: var(--accent); }
        h1 { font-size: 18px; font-weight: 600; color: var(--text); letter-spacing: -0.01em; }
        .subtitle { font-size: 12px; color: var(--text-faint); margin-top: 2px; }
        .status-badge { display: inline-flex; align-items: center; gap: 6px; padding: 6px 14px; border-radius: 100px; font-size: 12px; font-weight: 600;
            transition: background-color 200ms var(--ease), color 200ms var(--ease); }
        .status-badge .dot { width: 6px; height: 6px; border-radius: 50%; }
        .status-badge.ok { background: var(--ok-dim); color: var(--ok); }
        .status-badge.ok .dot { background: var(--ok); box-shadow: 0 0 8px var(--ok); animation: breathe 2.4s ease-in-out infinite; }
        .status-badge.degraded { background: var(--warn-dim); color: var(--warn); }
        .status-badge.degraded .dot { background: var(--warn); box-shadow: 0 0 8px var(--warn); }
        @keyframes breathe { 0%, 100% { opacity: 1; } 50% { opacity: 0.45; } }

        .grid-kpi { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 28px; }
        .card {
            background: var(--bg-elevated); border: 1px solid var(--border); border-radius: 10px; padding: 16px 18px;
            box-shadow: var(--shadow-sm); transition: transform 200ms var(--ease), box-shadow 200ms var(--ease), border-color 200ms var(--ease);
        }
        .card:hover { transform: translateY(-2px); box-shadow: var(--shadow-md); border-color: #2a3241; }
        .card-title { font-size: 11px; font-weight: 600; color: var(--text-faint); text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 10px; }
        .card-value { font-size: 26px; font-weight: 650; color: var(--text); font-family: var(--mono); letter-spacing: -0.02em; font-variant-numeric: tabular-nums;
            transition: color 200ms var(--ease); border-radius: 4px; }
        .card-value.small { font-size: 20px; }
        .card-value.pulse { animation: value-pulse 500ms var(--ease); }
        @keyframes value-pulse { 0% { color: var(--accent); } 100% { color: var(--text); } }

        section {
            background: var(--bg-elevated); border: 1px solid var(--border); border-radius: 10px; margin-bottom: 20px; overflow: hidden;
            box-shadow: var(--shadow-sm); transition: border-color 200ms var(--ease);
        }
        .section-header { display: flex; align-items: baseline; gap: 8px; padding: 14px 18px; border-bottom: 1px solid var(--border); font-size: 13px; font-weight: 600; color: var(--text); line-height: 1; }
        .section-header .count { color: var(--text-faint); font-weight: 500; font-size: 12px; font-variant-numeric: tabular-nums; }

        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th { text-align: left; color: var(--text-faint); font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; padding: 10px 18px; border-bottom: 1px solid var(--border); }
        td { padding: 11px 18px; border-bottom: 1px solid var(--border); color: var(--text); font-family: var(--mono); font-size: 12.5px; font-variant-numeric: tabular-nums; }
        tbody tr:last-child td { border-bottom: none; }
        tbody tr { transition: background-color 150ms var(--ease); }
        tbody tr:hover { background: rgba(255,255,255,0.025); }
        td.name { font-family: var(--sans); font-weight: 500; }
        td.num { color: var(--text-dim); }

        .badge { display: inline-flex; align-items: center; gap: 5px; padding: 3px 9px; border-radius: 100px; font-size: 11px; font-weight: 600; font-family: var(--sans);
            transition: background-color 200ms var(--ease), color 200ms var(--ease); }
        .badge::before { content: ''; width: 5px; height: 5px; border-radius: 50%; }
        .badge.active { background: var(--ok-dim); color: var(--ok); }
        .badge.active::before { background: var(--ok); }
        .badge.cooling { background: var(--warn-dim); color: var(--warn); }
        .badge.cooling::before { background: var(--warn); }
        .badge.off { background: rgba(120, 130, 143, 0.12); color: var(--text-faint); }
        .badge.off::before { background: var(--text-faint); }
        .badge.limit { background: var(--danger-dim); color: var(--danger); }
        .badge.limit::before { background: var(--danger); }

        th.chk-col, td.chk-col { width: 32px; padding-right: 0; }
        input[type="checkbox"] { width: 15px; height: 15px; accent-color: var(--accent); cursor: pointer; transition: transform 120ms var(--ease); }
        input[type="checkbox"]:active { transform: scale(0.88); }

        button { font-family: var(--sans); cursor: pointer; border: none; border-radius: 6px; font-weight: 600; transition: filter 150ms var(--ease), transform 100ms var(--ease), opacity 150ms var(--ease), color 150ms var(--ease), border-color 150ms var(--ease); }
        button:active:not(:disabled) { transform: scale(0.96); }
        .remove-key, .remove-model { background: transparent; color: var(--text-faint); border: 1px solid var(--border); padding: 4px 10px; font-size: 11px; }
        .remove-key:hover:not(:disabled), .remove-model:hover:not(:disabled) { color: var(--danger); border-color: var(--danger); }
        .remove-key:disabled, .remove-model:disabled { opacity: 0.35; cursor: not-allowed; transform: none; }

        #add-key-form, #add-model-form { display: flex; gap: 8px; padding: 14px 18px; border-top: 1px solid var(--border); }
        #add-key-input, #add-model-input { flex: 1; background: var(--bg); border: 1px solid var(--border); border-radius: 6px; padding: 8px 12px; color: var(--text); font-family: var(--mono); font-size: 12.5px;
            transition: border-color 150ms var(--ease), box-shadow 150ms var(--ease); }
        #add-key-input:focus, #add-model-input:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-dim); }
        #add-key-form button[type="submit"], #add-model-form button[type="submit"] { background: var(--accent); color: #0a0e14; padding: 8px 16px; font-size: 12.5px; }
        #add-key-form button[type="submit"]:hover:not(:disabled), #add-model-form button[type="submit"]:hover:not(:disabled) { filter: brightness(1.1); }
        #add-key-form button[type="submit"]:disabled, #add-model-form button[type="submit"]:disabled { opacity: 0.6; cursor: not-allowed; }
        .form-footer { display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 0 18px 14px; flex-wrap: wrap; }
        .form-error { color: var(--danger); font-size: 12px; }
        .get-key-link { color: var(--text-faint); font-size: 11.5px; text-decoration: none; transition: color 150ms var(--ease); }
        .get-key-link:hover { color: var(--accent); text-decoration: underline; }

        footer { text-align: center; color: var(--text-faint); font-size: 11px; margin-top: 32px; }
        @media (max-width: 560px) {
            body { padding: 24px 14px 48px; }
            .mobile-hide { display: none; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="title-group">
                <span class="logo"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2 3 14h7l-1 8 11-13h-7l1-7z"/></svg></span>
                <div>
                    <h1>Gemini Failover Proxy</h1>
                    <div class="subtitle">OpenCode local gateway · <span id="host-label"></span></div>
                </div>
            </div>
            <span class="status-badge ok" id="status-badge"><span class="dot"></span><span id="status-text">Healthy</span></span>
        </header>
        <div class="grid-kpi">
            <div class="card"><div class="card-title">Requests</div><div class="card-value" id="kpi-requests">0</div></div>
            <div class="card"><div class="card-title">Success Rate</div><div class="card-value" id="kpi-success-rate">100%</div></div>
            <div class="card"><div class="card-title">Active Keys</div><div class="card-value" id="kpi-keys">0</div></div>
            <div class="card"><div class="card-title">In Flight</div><div class="card-value" id="kpi-inflight">0</div></div>
            <div class="card"><div class="card-title">Uptime</div><div class="card-value small" id="kpi-uptime">0m 0s</div></div>
        </div>
        <section>
            <div class="section-header">Models <span class="count" id="models-count"></span></div>
            <table>
                <thead><tr><th class="chk-col"></th><th>Model</th><th>Status</th><th>Today</th><th class="mobile-hide">Attempts</th><th>OK</th><th>Avg Latency</th><th></th></tr></thead>
                <tbody id="models-tbody"></tbody>
            </table>
            <form id="add-model-form">
                <input id="add-model-input" type="text" placeholder="Add a model ID (e.g. a pro-tier model your plan grants access to)…" autocomplete="off" spellcheck="false">
                <button type="submit">Add model</button>
            </form>
            <div class="form-footer">
                <div class="form-error" id="model-error"></div>
            </div>
        </section>
        <section>
            <div class="section-header">API Keys <span class="count" id="keys-count"></span></div>
            <table>
                <thead><tr><th>Key</th><th>Status</th><th>Today</th><th class="mobile-hide">Attempts</th><th>OK</th><th>Avg Latency</th><th></th></tr></thead>
                <tbody id="keys-tbody"></tbody>
            </table>
            <form id="add-key-form">
                <input id="add-key-input" type="password" placeholder="Paste a new Google AI Studio API key…" autocomplete="off" spellcheck="false">
                <button type="submit">Add key</button>
            </form>
            <div class="form-footer">
                <div class="form-error" id="key-error"></div>
                <a class="get-key-link" href="https://aistudio.google.com/apikey" target="_blank" rel="noopener">Get a free API key on Google AI Studio ↗</a>
            </div>
        </section>
        <footer>Auto-refreshing every 2s</footer>
    </div>
    <script>
        // Read the port off the page rather than baking one in — PROXY_PORT is configurable.
        document.getElementById('host-label').textContent = location.host;

        function statusBadge(row) {
            if (!row) return '<span class="badge off">disabled</span>';
            if (row.daily_limited) return '<span class="badge limit">daily limit — resets in ' + formatDuration(row.resets_in_s) + '</span>';
            return row.cooling
                ? '<span class="badge cooling">cooling ' + row.retry_in_s + 's</span>'
                : '<span class="badge active">active</span>';
        }
        function formatDuration(s) {
            if (s == null) return '?';
            const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
            return h > 0 ? h + 'h ' + m + 'm' : m + 'm';
        }
        function cell(row, field) { return row ? row[field] : 0; }

        function renderModels(config, metrics) {
            const byName = new Map((metrics.models || []).map(m => [m.model, m]));
            document.getElementById('models-count').textContent = config.enabledModels.length + '/' + config.allModels.length;
            document.getElementById('models-tbody').innerHTML = config.allModels.map(name => {
                const m = byName.get(name);
                const checked = config.enabledModels.includes(name) ? 'checked' : '';
                const custom = !config.defaultModels.includes(name);
                const removeCell = custom
                    ? '<button class="remove-model" data-name="' + name + '">Remove</button>'
                    : '';
                return '<tr><td class="chk-col"><input type="checkbox" class="model-toggle" value="' + name + '" ' + checked + '></td>' +
                    '<td class="name">' + name + '</td><td>' + statusBadge(m) +
                    '</td><td class="num">' + (m ? m.requests_today : 0) +
                    '</td><td class="num mobile-hide">' + cell(m, 'attempts') + '</td><td class="num">' + cell(m, 'ok') +
                    '</td><td class="num">' + (cell(m, 'avg_latency_ms') || '-') + 'ms</td><td>' + removeCell + '</td></tr>';
            }).join('');
            document.querySelectorAll('.model-toggle').forEach(box => box.addEventListener('change', onModelToggle));
            document.querySelectorAll('.remove-model').forEach(btn => btn.addEventListener('click', onRemoveModel));
        }

        function renderKeys(config, metrics) {
            const metricsKeys = metrics.keys || [];
            document.getElementById('keys-count').textContent = config.keys.length;
            document.getElementById('keys-tbody').innerHTML = config.keys.map((k, i) => {
                const m = metricsKeys[i];
                const removeDisabled = config.keys.length <= 1 ? 'disabled title="At least one key is required"' : '';
                return '<tr><td class="name">key#' + k.id + ' (' + k.masked + ')</td><td>' +
                    statusBadge({ cooling: k.cooling, retry_in_s: m ? m.retry_in_s : 0, daily_limited: k.daily_limited, resets_in_s: k.resets_in_s }) +
                    '</td><td class="num">' + k.requests_today + '</td><td class="num mobile-hide">' + cell(m, 'attempts') + '</td><td class="num">' + cell(m, 'ok') +
                    '</td><td class="num">' + (cell(m, 'avg_latency_ms') || '-') + 'ms</td>' +
                    '<td><button class="remove-key" data-id="' + k.id + '" ' + removeDisabled + '>Remove</button></td></tr>';
            }).join('');
            document.querySelectorAll('.remove-key').forEach(btn => btn.addEventListener('click', onRemoveKey));
        }

        async function onModelToggle() {
            const models = Array.from(document.querySelectorAll('.model-toggle:checked')).map(b => b.value);
            if (!models.length) { this.checked = true; alert('At least one model must stay enabled.'); return; }
            await fetch('/api/models', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ models }) });
            updateDashboard();
        }

        async function onRemoveKey() {
            if (!confirm('Remove this key?')) return;
            const res = await fetch('/api/keys/' + this.dataset.id, { method: 'DELETE' });
            if (!res.ok) { const e = await res.json().catch(() => ({})); alert(e.error || 'Could not remove key'); }
            updateDashboard();
        }

        async function onRemoveModel() {
            if (!confirm('Remove "' + this.dataset.name + '" from the catalog?')) return;
            const res = await fetch('/api/models/' + encodeURIComponent(this.dataset.name), { method: 'DELETE' });
            if (!res.ok) { const e = await res.json().catch(() => ({})); alert(e.error || 'Could not remove model'); }
            updateDashboard();
        }

        async function withLoading(button, busyText, task) {
            const original = button.textContent;
            button.disabled = true; button.textContent = busyText;
            try { await task(); } finally { button.disabled = false; button.textContent = original; }
        }

        document.getElementById('add-key-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const input = document.getElementById('add-key-input');
            const errBox = document.getElementById('key-error');
            errBox.textContent = '';
            await withLoading(e.submitter, 'Adding…', async () => {
                const res = await fetch('/api/keys', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: input.value }) });
                if (res.ok) { input.value = ''; await updateDashboard(); }
                else { const e2 = await res.json().catch(() => ({})); errBox.textContent = e2.error || 'Could not add key'; }
            });
        });

        document.getElementById('add-model-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const input = document.getElementById('add-model-input');
            const errBox = document.getElementById('model-error');
            errBox.textContent = '';
            await withLoading(e.submitter, 'Adding…', async () => {
                const res = await fetch('/api/models', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ model: input.value.trim() }) });
                if (res.ok) { input.value = ''; await updateDashboard(); }
                else { const e2 = await res.json().catch(() => ({})); errBox.textContent = e2.error || 'Could not add model'; }
            });
        });

        // Flash a KPI value only when it actually changes — motion signals the
        // update instead of the number silently jumping between polls.
        function setKpi(id, value) {
            const el = document.getElementById(id);
            if (el.textContent === value) return;
            el.textContent = value;
            el.classList.remove('pulse'); void el.offsetWidth; el.classList.add('pulse');
        }

        async function updateDashboard() {
            const [metricsRes, configRes] = await Promise.all([fetch('/metrics'), fetch('/api/config')]);
            if (!metricsRes.ok || !configRes.ok) return;
            const data = await metricsRes.json();
            const config = await configRes.json();

            setKpi('kpi-requests', String(data.totals.requests));
            setKpi('kpi-success-rate', (data.totals.requests > 0 ? Math.round((data.totals.ok / data.totals.requests) * 100) : 100) + '%');
            setKpi('kpi-keys', String(config.keys.length));
            setKpi('kpi-inflight', data.bucket.in_flight + '/' + data.bucket.max_concurrent);
            setKpi('kpi-uptime', Math.floor(data.uptime_s / 60) + 'm ' + (data.uptime_s % 60) + 's');

            const badge = document.getElementById('status-badge');
            const text = document.getElementById('status-text');
            const cooling = data.models.filter(m => m.cooling).length;
            if (cooling > 0) { badge.className = 'status-badge degraded'; text.textContent = 'Degraded — ' + cooling + ' cooling'; }
            else { badge.className = 'status-badge ok'; text.textContent = 'Healthy'; }

            renderModels(config, data);
            renderKeys(config, data);
        }
        setInterval(updateDashboard, 2000); updateDashboard();
    </script>
</body>
</html>`;

/* ------------------------------------------------------------------ */
/* Logging (size-bounded, crash-proof)                                */
/* ------------------------------------------------------------------ */
const LOG_FILE = path.join(__dirname, 'proxy.log');
const LOG_BACKUP = path.join(__dirname, 'proxy.log.1');
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
/* .env loader                                                         */
/* ------------------------------------------------------------------ */
try {
  const envPath = path.join(__dirname, '.env');
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

/* ------------------------------------------------------------------ */
/* Daily usage tracking (per key+model — Google's RPD quota is scoped   */
/* per model per key, not per key as a whole). Google doesn't expose a  */
/* "remaining quota" API for AI Studio keys, so this is our own count of */
/* requests sent through this proxy, resetting at midnight PACIFIC TIME  */
/* — Google's documented RPD reset boundary, NOT UTC — not "since you   */
/* last checked".                                                       */
/* ------------------------------------------------------------------ */
const USAGE_FILE = path.join(__dirname, 'usage.json');
const PACIFIC_DATE_FMT = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Los_Angeles', year: 'numeric', month: '2-digit', day: '2-digit' });
function todayPacific() { return PACIFIC_DATE_FMT.format(new Date()); }
let usage = { day: todayPacific(), counts: {} }; // counts[keyId][model] = n
try {
  const raw = JSON.parse(fs.readFileSync(USAGE_FILE, 'utf8'));
  if (raw && raw.day === todayPacific()) usage = raw;
} catch { /* no usage file yet, or stale day — start fresh */ }

function bumpDailyUsage(keyId, model) {
  if (usage.day !== todayPacific()) { usage = { day: todayPacific(), counts: {} }; }
  // Self-heal: an older proxy version stored counts[keyId] as a flat number.
  if (typeof usage.counts[keyId] !== 'object' || usage.counts[keyId] === null) usage.counts[keyId] = {};
  const perKey = usage.counts[keyId];
  perKey[model] = (perKey[model] || 0) + 1;
  try { fs.writeFileSync(USAGE_FILE, JSON.stringify(usage)); } catch (e) { log('usage write error', e.message); }
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

/* ------------------------------------------------------------------ */
/* Dashboard-driven config mutations — persisted back to .env           */
/* ------------------------------------------------------------------ */
function saveEnvConfig() {
  const envPath = path.join(__dirname, '.env');
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
  const customModels = ALL_MODELS.filter(m => !DEFAULT_MODELS.includes(m));
  if (customModels.length) kept.push(`GOOGLE_CUSTOM_MODELS=${customModels.join(',')}`);
  // Write-then-rename: a crash mid-write leaves the old .env intact instead
  // of a truncated file that drops every key on next load.
  try {
    const tmpPath = envPath + '.tmp';
    fs.writeFileSync(tmpPath, kept.join('\n') + '\n');
    fs.renameSync(tmpPath, envPath);
  } catch (e) { log('env write error', e.message); }
}

function addKey(rawKey) {
  const key = String(rawKey || '').trim();
  if (!key) throw new Error('key is required');
  if (keyState.some(k => k.key === key)) throw new Error('key already exists');
  keyState.push({ id: nextKeyId++, key, failures: 0, until: 0 });
  saveEnvConfig();
  log(`key added: key#${keyState[keyState.length - 1].id} (${maskKey(key)})`);
}

function removeKey(id) {
  if (keyState.length <= 1) throw new Error('cannot remove the last key');
  const idx = keyState.findIndex(k => k.id === id);
  if (idx < 0) throw new Error('key not found');
  const [removed] = keyState.splice(idx, 1);
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
  saveEnvConfig();
  log(`custom model removed from catalog: ${id}`);
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

/* Rate shaping */
const COOLDOWN_BASE_MS = 30000;
const COOLDOWN_MAX_MS = 300000;
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
}

function markExhausted(model, daily = false) {
  const s = modelState.get(model) || { until: 0, failures: 0, lastUsedAt: 0 };
  s.failures += 1;
  s.until = daily
    ? Date.now() + msUntilPacificMidnight()
    : Date.now() + Math.min(COOLDOWN_BASE_MS * 2 ** (s.failures - 1), COOLDOWN_MAX_MS);
  s.dailyLimited = daily;
  modelState.set(model, s);
  const m = mm(model);
  m.cooldowns++;
  m.lastCooldownAt = Date.now();
  log(`cooldown ${model} for ${Math.round((s.until - Date.now()) / 1000)}s` +
    (daily ? ' (daily quota — parked until Pacific midnight)' : ` (failure #${s.failures})`));
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

function markKeyExhausted(idx, daily = false) {
  const k = keyState[idx];
  k.failures += 1;
  k.until = daily
    ? Date.now() + msUntilPacificMidnight()
    : Date.now() + Math.min(KEY_COOLDOWN_BASE_MS * 2 ** (k.failures - 1), KEY_COOLDOWN_MAX_MS);
  k.dailyLimited = daily;
  const m = km(idx);
  m.cooldowns++;
  m.lastCooldownAt = Date.now();
  log(`cooldown key#${k.id} (${maskKey(k.key)}) for ${Math.round((k.until - Date.now()) / 1000)}s` +
    (daily ? ' (daily quota — parked until Pacific midnight)' : ` (failure #${k.failures})`));
}

function markKeyHealthy(idx) {
  const k = keyState[idx];
  k.failures = 0;
  k.until = 0;
  k.dailyLimited = false;
}

/* Scope-aware exhaustion: always cool the model; cool the key ONLY on
 * key-wide evidence. This is what makes rotation across per-model quotas work.
 * Free-tier RPD (requests-per-day) is documented PER MODEL per key — that's
 * the common case — so a day-scoped 429 mostly parks just the model. A 429
 * that's ALSO key-wide (rare: worker-total / per_project evidence) parks the
 * key too. Either way, day-scoped exhaustion won't recover within a short
 * exponential cooldown, so it's parked until the actual Pacific-time daily reset. */
function coolOnExhaustion(model, keyIdx, parsed, errMsg) {
  const daily = isDailyQuota(parsed, errMsg);
  markExhausted(model, daily);
  if (quotaScope(parsed, errMsg) === 'key') {
    markKeyExhausted(keyIdx, daily);
    log(`key-wide exhaustion — key#${keyState[keyIdx].id} cooling too`);
  }
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
    if (msg.role !== 'assistant' || !Array.isArray(msg.tool_calls)) continue;
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

async function relaySse(upstreamRes, res, model, onExhausted, guard, onFirstByte) {
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
            else if (!inspectEvent(dataField)) safeWrite(`data: ${patchStreamEvent(dataField, streamState)}\n\n`);
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
    if (dataField !== null && !inspectEvent(dataField)) safeWrite(`data: ${patchStreamEvent(dataField, streamState)}\n\n`);
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
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(DASHBOARD_HTML);
    return;
  }

  // Dashboard control-panel API — keys and enabled models.
  if (req.url === '/api/config' && req.method === 'GET') {
    const now = Date.now();
    sendJson(res, 200, {
      allModels: ALL_MODELS,
      defaultModels: DEFAULT_MODELS,
      enabledModels: GOOGLE_MODELS,
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
      addKey(body.key);
      sendJson(res, 200, { ok: true });
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

  if (req.url.startsWith('/api/models/') && req.method === 'DELETE') {
    try {
      removeCustomModel(decodeURIComponent(req.url.slice('/api/models/'.length)));
      sendJson(res, 200, { ok: true });
    } catch (e) { sendJson(res, 400, { error: e.message }); }
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
    sendJson(res, 200, {
      uptime_s: Math.floor((now - STARTED_AT) / 1000),
      totals,
      bucket: {
        tokens: Math.round(bucketTokens * 1000) / 1000,
        refill_per_min: Math.round(60000 / bucketRateMs()),
        in_flight: googleInFlight,
        max_concurrent: MAX_CONCURRENT_GOOGLE,
      },
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
        };
      }),
      models: GOOGLE_MODELS.map(m => {
        const s = modelMetrics.get(m) || {};
        const st = modelState.get(m) || {};
        const isCooling = !!(st.until && st.until > now);
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

    log(`request: model=${requestedModel} stream=${isStreaming} msgs=${messages.length} tools=${!!rawPayload.tools} ctx=${cleanPayload.messages.length}`);
    totals.requests++;

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

    // Sleep in keepalive-sized slices. Returns false if the client vanished.
    const holdForQuota = async (ms, reason) => {
      // A short wait rides out on the unopened response: staying unopened keeps
      // a real status code + Retry-After available if this ends up failing.
      // Only a genuinely long hold is worth committing to a stream for.
      if (ms <= KEEPALIVE_MS) {
        log(`${reason}: waiting ${ms}ms`);
        await sleep(ms);
        return !clientGone;
      }
      const until = Date.now() + ms;
      log(`${reason} — holding request ${Math.round(ms / 1000)}s for quota (wait-for-quota)`);
      openKeepAlive();
      while (Date.now() < until) {
        if (clientGone || res.writableEnded || res.destroyed) {
          log('client disconnected while waiting for quota — abandoning request');
          return false;
        }
        await sleep(Math.min(KEEPALIVE_MS, until - Date.now()));
        // `:` lines are SSE comments — ignored by parsers, but they keep the
        // socket (and the client's idle timer) alive.
        if (keepAliveOpen && !res.writableEnded && !res.destroyed) res.write(': waiting-for-quota\n\n');
      }
      return true;
    };

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

      try {
        const payload = { ...cleanPayload, model };
        const attemptStart = Date.now();
        const upstreamRes = await attemptGoogle(payload, isStreaming, guard, apiKey);

        const ct = (upstreamRes.headers.get('content-type') || '').toLowerCase();
        const isSse = ct.includes('text/event-stream');

        if (isSse) {
          // True streaming path — relay verbatim; detect errors per-event.
          await relaySse(upstreamRes, res, model,
            (m, errObj) => coolOnExhaustion(m, keyIdx, { error: errObj }, JSON.stringify(errObj || '')),
            guard, () => { realDataSent = true; });
          markHealthy(model);
          markKeyHealthy(keyIdx);
          recordOk(model, attemptStart);
          recordKeyOk(keyIdx, attemptStart);
          log(`stream done via ${model} (key#${keyState[keyIdx].id})`);
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
          coolOnExhaustion(model, keyIdx, parsed, errMsg);
          const retryAfter = upstreamRes.headers.get('retry-after');
          log(`exhausted on ${model} (${upstreamRes.status}): ${errMsg.slice(0, 200)} retry-after=${retryAfter}`);
          continue; // rotate with cooldown
        }

        if (upstreamRes.status === 400) {
          if (AUTH_ERROR_RE.test(errMsg)) {
            // Dead/revoked key — park it long and rotate, don't fail the request.
            markKeyExhausted(keyIdx);
            keyState[keyIdx].until = Date.now() + DEAD_KEY_PARK_MS;
            log(`key#${keyState[keyIdx].id} rejected as invalid — parked 30m, rotating`);
            continue;
          }
          // Deterministic payload error — fail fast, do not burn models.
          totals.failFast400++;
          log(`400 from ${model}: ${errMsg.slice(0, 300)}`);
          // If we already opened a keepalive stream, a status code is no longer
          // available — report it in-stream instead of silently hanging.
          if (res.headersSent) safeEndSse(res, errMsg);
          else sendJson(res, 400, { error: { message: errMsg, status: 400 } });
          return;
        }

        if (!upstreamRes.ok || parsed?.error || !parsed?.choices?.length) {
          totals.upstreamError++;
          log(`upstream error ${upstreamRes.status} from ${model}: ${errMsg.slice(0, 300)}`);
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
          }
          res.write('data: [DONE]\n\n');
          res.end();
          totals.synthesizedStream++;
          log(`synthesized stream from JSON via ${model}`);
          return;
        }

        addToolCallIndices(parsed.choices[0].message?.tool_calls);
        correctFinishReason(parsed.choices[0], parsed.choices[0].message?.tool_calls);
        sendJson(res, 200, parsed);
        log(`ok via ${model}`);
        return;

      } catch (err) {
        const aborted = guard.signal.aborted;
        mm(model).attemptErrors++;
        km(keyIdx).attemptErrors++;
        log(`attempt ${model} failed: ${err.message} aborted=${aborted}`);

        if (aborted && realDataSent) {
          // Mid-stream abort after real model bytes sent — terminate cleanly, no rotation.
          safeEndSse(res, 'Upstream stalled mid-stream');
          return;
        }
        if (aborted && (err.name === 'AbortError' || err.message.includes('abort'))) {
          // Timeout before headers — per-model stall, rotate (key presumably fine).
          markExhausted(model);
          continue;
        }
        // Network error — per-model cooldown, rotate (key presumably fine).
        markExhausted(model);
        continue;
      } finally {
        guard.clearAll();
        res.off('close', abortOnClose);
        releaseGoogle();
      }
    }

    /* ---- All Google models exhausted ---- */
    totals.final429++;
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
    const giveUpMessage = anyDaily
      ? `All enabled Gemini models/keys are rate-limited, and at least one has hit its daily quota (resets at Pacific midnight, ~${Math.ceil(soonestS / 60)}m). Enable another model, add a key, or wait for the reset.`
      : 'All Gemini models are currently rate-limited (free-tier worker limit). Try again in a few seconds.';
    // Only reachable with wait-for-quota off, or after the full wait budget
    // elapsed. If a keepalive stream is already open there's no status code
    // left to send, so the failure goes in-stream.
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

server.listen(PORT, '127.0.0.1', () => {
  log(`[Passthrough Failover Proxy] listening on http://localhost:${PORT}`);
  log(`[Passthrough Failover Proxy] google models: ${GOOGLE_MODELS.join(', ')}`);
  log(`[Passthrough Failover Proxy] api keys: ${keyState.length} (${keyState.map(k => `key#${k.id} ${maskKey(k.key)}`).join(', ')})`);
});
