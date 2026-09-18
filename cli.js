#!/usr/bin/env node
/**
 * cli.js — `npx opencode-gemini-proxy` entry point.
 *
 * Commands:
 *   (none) | start    run the proxy (delegates to server.js)
 *   init              interactive one-time setup (key, port, .env, OpenCode
 *                     provider config, TUI plugin)
 *   doctor            environment/health report for bug filing
 *
 * Zero dependencies — only node:* builtins.
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createInterface } from 'node:readline/promises';
import { resolveDataDir } from './paths.js';

const __filename = fileURLToPath(import.meta.url);
const moduleDir = path.dirname(__filename);

const maskKey = (k) => (k && k.length > 8 ? `...${k.slice(-4)}` : '...(short)');

/* ------------------------------------------------------------------ */
/* Pure helpers — exported so test.js can exercise them directly.      */
/* ------------------------------------------------------------------ */

// The exact provider block from README.md's "Configure OpenCode" section.
function googleProviderBlock(port) {
  return {
    npm: '@ai-sdk/openai-compatible',
    name: 'Google Auto-Failover Proxy (Local)',
    options: { baseURL: `http://localhost:${port}/v1` },
    models: {
      'gemini-3.8-flash': { name: 'Gemini 3.8 Flash (Auto-Failover)', multimodal: true, reasoning: true, tool_call: true },
      'gemini-3.7-flash': { name: 'Gemini 3.7 Flash (Auto-Failover)', multimodal: true, reasoning: true, tool_call: true },
      'gemini-3.6-flash': { name: 'Gemini 3.6 Flash (Auto-Failover)', multimodal: true, reasoning: true, tool_call: true },
      'gemini-3.5-flash': { name: 'Gemini 3.5 Flash (Auto-Failover)', multimodal: true, reasoning: true, tool_call: true },
      'gemini-3.5-flash-lite': { name: 'Gemini 3.5 Flash Lite (Auto-Failover)', multimodal: true, reasoning: true, tool_call: true },
      'gemini-3.1-flash-lite': { name: 'Gemini 3.1 Flash Lite (Auto-Failover)', multimodal: true, reasoning: true, tool_call: true },
    },
  };
}

// existingJsonObject: parsed opencode.json, or undefined/null if the file
// doesn't exist yet. Never touches anything but provider.google and (only
// if absent) the top-level model/small_model defaults.
export function mergeOpenCodeConfig(existingJsonObject, port) {
  const cfg = existingJsonObject && typeof existingJsonObject === 'object'
    ? { ...existingJsonObject }
    : { $schema: 'https://opencode.ai/config.json' };
  cfg.provider = { ...(cfg.provider || {}), google: googleProviderBlock(port) };
  if (cfg.model === undefined) cfg.model = 'google/gemini-3.8-flash';
  if (cfg.small_model === undefined) cfg.small_model = 'google/gemini-3.5-flash-lite';
  return cfg;
}

// Next free GOOGLE_API_KEY_N slot in an existing .env's text, so adding a
// key appends rather than clobbers. Legacy unnumbered GOOGLE_API_KEY counts
// as slot 1.
export function nextKeySlot(envText) {
  const text = String(envText || '');
  const taken = new Set();
  for (const m of text.matchAll(/^GOOGLE_API_KEY_(\d+)\s*=/gm)) taken.add(Number(m[1]));
  if (/^GOOGLE_API_KEY\s*=\S/m.test(text)) taken.add(1);
  let n = 1;
  while (taken.has(n)) n++;
  return n;
}

/* ------------------------------------------------------------------ */
/* Small utilities                                                     */
/* ------------------------------------------------------------------ */

async function fetchWithTimeout(url, ms) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

// Standalone key check (init runs before the server exists, so this can't
// reuse server.js's verifyGoogleKey). Same contract: reject only on a clear
// bad-key answer, let network failures through with a warning.
async function verifyKey(key) {
  try {
    const res = await fetchWithTimeout(
      `https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(key)}&pageSize=1`,
      8000,
    );
    const text = await res.text();
    let parsed = null;
    try { parsed = JSON.parse(text); } catch { /* not JSON */ }
    if (res.status === 400 || res.status === 403) {
      const blob = `${parsed?.error?.status || ''} ${parsed?.error?.message || text}`;
      if (/API_KEY_INVALID|PERMISSION_DENIED/i.test(blob)) {
        return { ok: false, rejected: true, message: (parsed?.error?.message || text || 'key rejected').slice(0, 200) };
      }
    }
    return { ok: true };
  } catch (e) {
    return { ok: true, offline: true, message: e.message };
  }
}

function hasKeyEnvVars(env) {
  if (env.GOOGLE_API_KEY || env.GOOGLE_API_KEYS) return true;
  for (let i = 1; i <= 10; i++) if (env[`GOOGLE_API_KEY_${i}`]) return true;
  return false;
}

function currentDataDir() {
  return resolveDataDir({
    env: process.env,
    hasLocalEnv: fs.existsSync(path.join(moduleDir, '.env')),
    homedir: os.homedir(),
    moduleDir,
  });
}

function parseArgs(argv) {
  const opts = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--key') opts.key = argv[++i];
    else if (a === '--port') opts.port = Number(argv[++i]);
    else if (a === '--yes' || a === '-y') opts.yes = true;
    else if (a === '--no-opencode') opts.noOpencode = true;
    else if (a === '--no-plugin') opts.noPlugin = true;
    else if (a === '--skip-key-check') opts.skipKeyCheck = true;
    else if (a === '--help' || a === '-h') opts.help = true;
    else opts._.push(a);
  }
  return opts;
}

function printHelp() {
  console.log(`opencode-gemini-proxy — OpenAI-compatible failover proxy for Google Gemini

Usage:
  opencode-gemini-proxy [start]        run the proxy
  opencode-gemini-proxy init [opts]    one-time interactive setup
  opencode-gemini-proxy doctor         environment/health report

init options:
  --key <key>          Google AI Studio API key (skip the prompt)
  --port <n>            proxy port (default 8085)
  --yes, -y              accept defaults, skip confirmations (needs --key)
  --no-opencode          don't touch ~/.config/opencode/opencode.json
  --no-plugin            don't install the OpenCode TUI plugin
  --skip-key-check       don't validate the key against Google before saving
`);
}

/* ------------------------------------------------------------------ */
/* Commands                                                             */
/* ------------------------------------------------------------------ */

async function cmdStart() {
  const dataDir = currentDataDir();
  const envExists = fs.existsSync(path.join(dataDir, '.env'));
  if (!envExists && !hasKeyEnvVars(process.env)) {
    console.log('No config found. Run: npx opencode-gemini-proxy init');
  }
  await import('./server.js');
}

async function cmdInit(opts) {
  if (opts.yes && !opts.key) {
    console.error('init --yes requires --key (nothing to prompt for)');
    process.exit(1);
  }

  const dataDir = currentDataDir();
  fs.mkdirSync(dataDir, { recursive: true });

  const rl = opts.yes ? null : createInterface({ input: process.stdin, output: process.stdout });
  const ask = async (prompt) => (rl ? (await rl.question(prompt)).trim() : '');

  try {
    // (a) key
    let key = opts.key;
    if (!key) key = await ask('Google AI Studio API key (get one free: https://aistudio.google.com/apikey): ');
    if (!key) { console.error('A key is required.'); process.exit(1); }

    if (!opts.skipKeyCheck) {
      const check = await verifyKey(key);
      if (check.rejected) {
        console.error(`Key rejected by Google: ${check.message}`);
        process.exit(1);
      }
      if (check.offline) console.warn(`Warning: couldn't verify the key online (${check.message}) — continuing anyway.`);
    }
    console.log(`✔ API key ${maskKey(key)} accepted`);

    // (b) port
    let port = opts.port;
    if (!port) {
      const ans = await ask('Port to run the proxy on [8085]: ');
      port = ans ? Number(ans) : 8085;
    }
    if (!Number.isInteger(port) || port <= 0) port = 8085;
    console.log(`✔ Port set to ${port}`);

    // (c) .env
    const envPath = path.join(dataDir, '.env');
    const existingEnv = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf8') : null;
    if (existingEnv === null) {
      fs.writeFileSync(envPath, `PROXY_PORT=${port}\nGOOGLE_API_KEY_1=${key}\n`);
      console.log(`✔ Wrote ${envPath}`);
    } else {
      let append = opts.yes;
      if (!append) {
        const ans = await ask(`${envPath} already exists — append this key as a new slot? [y/N] `);
        append = /^y/i.test(ans);
      }
      if (append) {
        const slot = nextKeySlot(existingEnv);
        const sep = existingEnv.endsWith('\n') ? '' : '\n';
        fs.writeFileSync(envPath, `${existingEnv}${sep}GOOGLE_API_KEY_${slot}=${key}\n`);
        console.log(`✔ Appended key as GOOGLE_API_KEY_${slot} in ${envPath}`);
      } else {
        console.log('Skipped writing .env.');
      }
    }

    // (d) OpenCode provider config
    if (!opts.noOpencode) {
      const ocPath = path.join(os.homedir(), '.config', 'opencode', 'opencode.json');
      fs.mkdirSync(path.dirname(ocPath), { recursive: true });
      let existing;
      let jsonc = false;
      if (fs.existsSync(ocPath)) {
        const raw = fs.readFileSync(ocPath, 'utf8');
        try { existing = JSON.parse(raw); } catch { jsonc = true; }
      }
      if (jsonc) {
        console.log(`${ocPath} isn't plain JSON (comments/trailing commas?) — leaving it untouched. Paste this under "provider" yourself:`);
        console.log(JSON.stringify({ provider: { google: googleProviderBlock(port) } }, null, 2));
      } else {
        if (existing) {
          const backupPath = `${ocPath}.bak-${Date.now()}`;
          fs.copyFileSync(ocPath, backupPath);
          console.log(`✔ Backed up existing opencode.json to ${backupPath}`);
        }
        const merged = mergeOpenCodeConfig(existing, port);
        fs.writeFileSync(ocPath, JSON.stringify(merged, null, 2) + '\n');
        console.log(`✔ Wrote provider config to ${ocPath}`);
      }
    }

    // (e) TUI plugin
    if (!opts.noPlugin) {
      const src = path.join(moduleDir, 'opencode-plugin', 'gemini-proxy.js');
      const destDir = path.join(os.homedir(), '.config', 'opencode', 'plugins');
      fs.mkdirSync(destDir, { recursive: true });
      const dest = path.join(destDir, 'gemini-proxy.js');
      fs.copyFileSync(src, dest);
      console.log(`✔ Installed OpenCode plugin to ${dest}`);
    }

    // (f) next steps
    console.log('\nNext steps:');
    console.log('  Start the proxy:  npx opencode-gemini-proxy start');
    console.log(`  Dashboard:        http://localhost:${port}`);
    console.log('  Restart OpenCode and pick google/gemini-3.8-flash');
  } finally {
    rl?.close();
  }
}

async function cmdDoctor() {
  console.log(`Node: ${process.version}`);
  console.log(`Platform: ${process.platform}`);

  const dataDir = currentDataDir();
  console.log(`Data dir: ${dataDir}`);

  const envPath = path.join(dataDir, '.env');
  const envExists = fs.existsSync(envPath);
  let port = Number(process.env.PROXY_PORT) || 8085;
  let maskedKeys = [];
  if (envExists) {
    const text = fs.readFileSync(envPath, 'utf8');
    for (const m of text.matchAll(/^GOOGLE_API_KEY(?:_\d+)?\s*=\s*(\S+)/gm)) maskedKeys.push(maskKey(m[1]));
    const portMatch = text.match(/^PROXY_PORT\s*=\s*(\d+)/m);
    if (portMatch && !process.env.PROXY_PORT) port = Number(portMatch[1]);
  }
  console.log(`.env: ${envExists ? `present (${envPath})` : `missing (${envPath})`}`);
  console.log(`Keys: ${maskedKeys.length} (${maskedKeys.join(', ') || 'none'})`);

  try {
    const res = await fetchWithTimeout(`http://127.0.0.1:${port}/health`, 3000);
    const body = await res.json();
    console.log(`Proxy /health: ${res.status} status=${body.status} cooling_models=${body.cooling?.length ?? 0} cooling_keys=${body.keys?.cooling?.length ?? 0}`);
  } catch (e) {
    console.log(`Proxy /health: unreachable (${e.message})`);
  }

  const ocPath = path.join(os.homedir(), '.config', 'opencode', 'opencode.json');
  if (fs.existsSync(ocPath)) {
    try {
      const cfg = JSON.parse(fs.readFileSync(ocPath, 'utf8'));
      const g = cfg.provider?.google;
      console.log(`opencode.json provider.google: ${g ? `present (baseURL ${g.options?.baseURL ?? 'unset'})` : 'missing'}`);
    } catch {
      console.log(`opencode.json: present but not valid JSON (JSONC?) at ${ocPath} — couldn't check provider.google`);
    }
  } else {
    console.log(`opencode.json: not found (${ocPath})`);
  }

  const pluginPath = path.join(os.homedir(), '.config', 'opencode', 'plugins', 'gemini-proxy.js');
  console.log(`Plugin: ${fs.existsSync(pluginPath) ? 'installed' : 'not installed'} (${pluginPath})`);

  const logPath = path.join(dataDir, 'proxy.log');
  if (fs.existsSync(logPath)) {
    const lines = fs.readFileSync(logPath, 'utf8').split('\n').filter(Boolean);
    const last50 = lines.slice(-50).map((l) => l.replace(/AIza[0-9A-Za-z_-]+/g, (m) => `...${m.slice(-4)}`));
    console.log(`\nLast ${last50.length} log lines (${logPath}):`);
    for (const l of last50) console.log(l);
  } else {
    console.log(`proxy.log: not found (${logPath})`);
  }

  process.exit(0);
}

/* ------------------------------------------------------------------ */
/* Entry                                                               */
/* ------------------------------------------------------------------ */

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const cmd = opts._[0] || 'start';

  if (opts.help || cmd === 'help') return printHelp();
  if (cmd === 'start') return cmdStart();
  if (cmd === 'init') return cmdInit(opts);
  if (cmd === 'doctor') return cmdDoctor();

  console.error(`Unknown command: ${cmd}\n`);
  printHelp();
  process.exit(1);
}

const isMain = (() => {
  try { return import.meta.url === pathToFileURL(process.argv[1] || '').href; } catch { return false; }
})();
if (isMain) main();
