/**
 * test.js — self-check for the quota-classification logic in quota.js.
 * Run: node --test
 * No framework, no fixtures — just node:test + node:assert. Covers the exact
 * bug class this proxy already hit twice: per-model vs per-key scope, and
 * daily (RPD) vs short (RPM) quota detection.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { quotaScope, isDailyQuota, msUntilPacificMidnight, retryDelayMs, learnedLimitFromTrip, quotaDimension } from './quota.js';
import { addToolCallIndices, correctFinishReason, patchStreamEvent } from './compat.js';
import { stripProxyStatusLines, pushRing } from './status.js';
import { resolveDataDir } from './paths.js';
import { mergeOpenCodeConfig, nextKeySlot } from './cli.js';
import { filterChatModels, sortByVersionDesc, diffCatalog } from './discovery.js';

const perModelRpm = {
  error: { details: [{ quotaId: 'GenerateRequestsPerMinutePerProjectPerModel-FreeTier', quotaMetric: 'per_model' }] },
};
const perModelRpd = {
  error: { details: [{ quotaId: 'GenerateRequestsPerDayPerProjectPerModel-FreeTier', quotaMetric: 'per_model' }] },
};
const perProjectRpd = {
  error: { details: [{ quotaId: 'GenerateRequestsPerDayPerProject-FreeTier', quotaMetric: 'per_project' }] },
};
const workerTotalText = 'worker local total request limit exceeded';

test('quotaScope: per_model detail scopes to model, not key', () => {
  assert.equal(quotaScope(perModelRpm, ''), 'model');
});

test('quotaScope: per_project detail scopes to key', () => {
  assert.equal(quotaScope(perProjectRpd, ''), 'key');
});

test('quotaScope: worker-total message text scopes to key even with no details', () => {
  assert.equal(quotaScope(null, workerTotalText), 'key');
});

test('quotaScope: unknown/empty error defaults to model (never silently parks a key)', () => {
  assert.equal(quotaScope({}, ''), 'model');
  assert.equal(quotaScope(null, ''), 'model');
});

test('isDailyQuota: per-model RPD detail is day-scoped', () => {
  assert.equal(isDailyQuota(perModelRpd, ''), true);
});

test('isDailyQuota: per-model RPM detail is NOT day-scoped', () => {
  assert.equal(isDailyQuota(perModelRpm, ''), false);
});

test('isDailyQuota: falls back to matching the raw error message text', () => {
  assert.equal(isDailyQuota(null, 'You exceeded your Requests Per Day quota'), true);
  assert.equal(isDailyQuota(null, 'rate limited, try again shortly'), false);
});

// Google resets Gemini's daily (RPD) quota at midnight PACIFIC TIME, not UTC
// — confirmed against Google's own docs. These instants are UTC timestamps
// chosen to land at known Pacific wall-clock times, including across the
// PST/PDT boundary, so a regression back to hardcoding a UTC-8 offset (or
// any fixed offset) fails obviously in summer.
test('msUntilPacificMidnight: counts down within the same Pacific day (winter, PST = UTC-8)', () => {
  // 2026-01-01T07:50:00Z == 2025-12-31 23:50:00 PST
  const ms = msUntilPacificMidnight(new Date(Date.UTC(2026, 0, 1, 7, 50, 0)));
  assert.equal(ms, 10 * 60 * 1000);
});

test('msUntilPacificMidnight: right at Pacific midnight rolls to the following day (24h), not 0 (winter)', () => {
  // 2026-01-01T08:00:00Z == 2026-01-01 00:00:00 PST
  const ms = msUntilPacificMidnight(new Date(Date.UTC(2026, 0, 1, 8, 0, 0)));
  assert.equal(ms, 24 * 60 * 60 * 1000);
});

test('msUntilPacificMidnight: same 10-minute gap in summer (PDT = UTC-7), not the winter offset', () => {
  // 2026-07-01T06:50:00Z == 2026-06-30 23:50:00 PDT
  const ms = msUntilPacificMidnight(new Date(Date.UTC(2026, 6, 1, 6, 50, 0)));
  assert.equal(ms, 10 * 60 * 1000);
});

/* ---- RetryInfo-aware retry delay (honors Google's exact cooldown) ---- */

test('retryDelayMs: reads the RetryInfo detail (whole seconds)', () => {
  const parsed = { error: { details: [{ '@type': 'type.googleapis.com/google.rpc.RetryInfo', retryDelay: '37s' }] } };
  assert.equal(retryDelayMs(parsed, null), 37000);
});

test('retryDelayMs: reads fractional seconds and falls back to the Retry-After header when no RetryInfo detail exists', () => {
  const parsed = { error: { details: [{ '@type': 'type.googleapis.com/google.rpc.RetryInfo', retryDelay: '12.5s' }] } };
  assert.equal(retryDelayMs(parsed, null), 12500);
  assert.equal(retryDelayMs({}, '5'), 5000);
});

test('retryDelayMs: returns null when neither source has a delay', () => {
  assert.equal(retryDelayMs(null, null), null);
  assert.equal(retryDelayMs({ error: { details: [] } }, undefined), null);
});

/* ---- learnedLimitFromTrip: RPD is per-key, not pooled across keys ---- */

test('learnedLimitFromTrip: learns the tripping key\'s own count, not the pool', () => {
  // 3 evenly-used keys each at 80 requests today (pool total 240) — the real
  // per-key RPD limit is 80, not 240.
  assert.equal(learnedLimitFromTrip({ 'gemini-3.8-flash': 80 }, 'gemini-3.8-flash'), 80);
});

test('learnedLimitFromTrip: returns null (don\'t overwrite a good learned value) when this key\'s count is 0', () => {
  assert.equal(learnedLimitFromTrip({}, 'gemini-3.8-flash'), null);
  assert.equal(learnedLimitFromTrip(undefined, 'gemini-3.8-flash'), null);
});

/* ---- quotaDimension: rpd / tpm / rpm / unknown classification ---- */

const rpdDetail = {
  error: { details: [{ quotaId: 'GenerateRequestsPerDayPerProjectPerModel-FreeTier', quotaMetric: 'generativelanguage.googleapis.com/generate_requests_per_model_per_day' }] },
};
const tpmDetail = {
  error: { details: [{ quotaId: 'GenerateContentInputTokensPerModelPerMinute-FreeTier', quotaMetric: 'generativelanguage.googleapis.com/generate_content_input_token_count' }] },
};
const rpmDetail = {
  error: { details: [{ quotaId: 'GenerateRequestsPerMinutePerProjectPerModel-FreeTier', quotaMetric: 'generativelanguage.googleapis.com/generate_requests_per_model' }] },
};

test('quotaDimension: PerDay detail classifies as rpd', () => {
  assert.equal(quotaDimension(rpdDetail, ''), 'rpd');
});

test('quotaDimension: token + PerMinute detail classifies as tpm', () => {
  assert.equal(quotaDimension(tpmDetail, ''), 'tpm');
});

test('quotaDimension: request + PerMinute detail classifies as rpm', () => {
  assert.equal(quotaDimension(rpmDetail, ''), 'rpm');
});

test('quotaDimension: falls back to scanning errMsg text for tokens-per-minute wording', () => {
  assert.equal(quotaDimension(null, 'You exceeded your tokens per minute quota'), 'tpm');
});

test('quotaDimension: no details and no matching text is unknown', () => {
  assert.equal(quotaDimension({}, ''), 'unknown');
  assert.equal(quotaDimension(null, 'generic rate limit'), 'unknown');
});

/* ---- OpenAI-spec compliance fixes for Google's compat endpoint ---- */

// A tool-calling turn must close with "tool_calls". Google's streaming path
// sends "stop", which tells a client the turn is OVER rather than "a tool run
// is needed and this turn continues" — OpenCode renders a fresh agent/model/
// duration header per tool call as a direct result.
const stream = (obj) => JSON.stringify(obj);
const toolCallChunk = (args) => stream({
  choices: [{ delta: { role: 'assistant', tool_calls: [{ id: 'call_1', type: 'function', function: { name: 'read', arguments: args } }] }, index: 0 }],
});
const finishChunk = (reason) => stream({ choices: [{ delta: { role: 'assistant' }, finish_reason: reason, index: 0 }] });

test('patchStreamEvent: rewrites finish_reason stop -> tool_calls once a tool call was seen', () => {
  const state = { nextIndex: 0, sawToolCall: false };
  patchStreamEvent(toolCallChunk('{}'), state);
  const out = JSON.parse(patchStreamEvent(finishChunk('stop'), state));
  assert.equal(out.choices[0].finish_reason, 'tool_calls');
});

test('patchStreamEvent: leaves finish_reason stop alone for a plain text turn', () => {
  const state = { nextIndex: 0, sawToolCall: false };
  const payload = finishChunk('stop');
  assert.equal(patchStreamEvent(payload, state), payload); // untouched, byte-for-byte
});

test('patchStreamEvent: never rewrites a non-stop finish_reason (length means length)', () => {
  const state = { nextIndex: 0, sawToolCall: false };
  patchStreamEvent(toolCallChunk('{}'), state);
  const out = JSON.parse(patchStreamEvent(finishChunk('length'), state));
  assert.equal(out.choices[0].finish_reason, 'length');
});

test('patchStreamEvent: numbers tool calls across chunks, not per chunk', () => {
  const state = { nextIndex: 0, sawToolCall: false };
  const a = JSON.parse(patchStreamEvent(toolCallChunk('{"a":1}'), state));
  const b = JSON.parse(patchStreamEvent(toolCallChunk('{"b":2}'), state));
  assert.equal(a.choices[0].delta.tool_calls[0].index, 0);
  assert.equal(b.choices[0].delta.tool_calls[0].index, 1);
});

test('patchStreamEvent: respects an index the upstream did send', () => {
  const state = { nextIndex: 0, sawToolCall: false };
  const withIndex = stream({ choices: [{ delta: { tool_calls: [{ index: 5, id: 'x', type: 'function', function: { name: 'f', arguments: '{}' } }] }, index: 0 }] });
  patchStreamEvent(withIndex, state);
  const next = JSON.parse(patchStreamEvent(toolCallChunk('{}'), state));
  assert.equal(next.choices[0].delta.tool_calls[0].index, 6); // continues after it
});

test('patchStreamEvent: passes through non-JSON and choice-less events unchanged', () => {
  const state = { nextIndex: 0, sawToolCall: false };
  assert.equal(patchStreamEvent('not json', state), 'not json');
  const usageOnly = stream({ choices: [], usage: { total_tokens: 5 } });
  assert.equal(patchStreamEvent(usageOnly, state), usageOnly);
});

test('addToolCallIndices: numbers by array position, leaving existing values alone', () => {
  const tcs = [{ id: 'a' }, { id: 'b', index: 9 }, { id: 'c' }];
  addToolCallIndices(tcs);
  assert.deepEqual(tcs.map((t) => t.index), [0, 9, 2]);
  assert.doesNotThrow(() => addToolCallIndices(undefined));
});

/* ---- status.js: replay stripping + event ring buffer ---- */

test('stripProxyStatusLines: drops proxy status lines, keeps the rest', () => {
  const text = 'Line one\n[gemini-proxy] gemini-3.8-flash rate-limited, trying gemini-3.7-flash\nLine two';
  assert.equal(stripProxyStatusLines(text), 'Line one\nLine two');
});

test('stripProxyStatusLines: returns null when nothing is left (field should be deleted)', () => {
  assert.equal(stripProxyStatusLines('[gemini-proxy] all models cooling\n[gemini-proxy] still waiting'), null);
  assert.equal(stripProxyStatusLines('   \n  '), null);
});

test('stripProxyStatusLines: non-string input passed through untouched', () => {
  assert.equal(stripProxyStatusLines(undefined), undefined);
  assert.equal(stripProxyStatusLines(null), null);
});

test('pushRing: keeps only the last `max` entries, oldest first', () => {
  const ring = [];
  for (let i = 0; i < 5; i++) pushRing(ring, { i }, 3);
  assert.deepEqual(ring.map(e => e.i), [2, 3, 4]);
});

/* ---- paths.js: data dir precedence ---- */

test('resolveDataDir: PROXY_DATA_DIR always wins', () => {
  const dir = resolveDataDir({ env: { PROXY_DATA_DIR: '/custom' }, hasLocalEnv: true, homedir: '/home/x', moduleDir: '/repo' });
  assert.equal(dir, '/custom');
});

test('resolveDataDir: a local .env (git checkout) uses the module dir', () => {
  const dir = resolveDataDir({ env: {}, hasLocalEnv: true, homedir: '/home/x', moduleDir: '/repo' });
  assert.equal(dir, '/repo');
});

test('resolveDataDir: no override and no local .env falls back to ~/.config/opencode-gemini-proxy', () => {
  const dir = resolveDataDir({ env: {}, hasLocalEnv: false, homedir: '/home/x', moduleDir: '/repo' });
  assert.equal(dir, path.join('/home/x', '.config', 'opencode-gemini-proxy'));
});

/* ---- cli.js: OpenCode config merge + .env key slot ---- */

test('mergeOpenCodeConfig: creates a fresh config with $schema and defaults when none exists', () => {
  const cfg = mergeOpenCodeConfig(undefined, 8085);
  assert.equal(cfg.$schema, 'https://opencode.ai/config.json');
  assert.equal(cfg.provider.google.options.baseURL, 'http://localhost:8085/v1');
  assert.equal(cfg.model, 'google/gemini-3.8-flash');
  assert.equal(cfg.small_model, 'google/gemini-3.5-flash-lite');
});

test('mergeOpenCodeConfig: preserves existing providers, models, and a custom top-level model', () => {
  const existing = { provider: { anthropic: { npm: '@ai-sdk/anthropic' } }, model: 'anthropic/claude', small_model: 'anthropic/haiku' };
  const cfg = mergeOpenCodeConfig(existing, 9000);
  assert.equal(cfg.provider.anthropic.npm, '@ai-sdk/anthropic');
  assert.ok(cfg.provider.google);
  assert.equal(cfg.provider.google.options.baseURL, 'http://localhost:9000/v1');
  // pre-existing top-level model/small_model are never overwritten
  assert.equal(cfg.model, 'anthropic/claude');
  assert.equal(cfg.small_model, 'anthropic/haiku');
});

test('nextKeySlot: finds the next free GOOGLE_API_KEY_N, filling gaps last', () => {
  assert.equal(nextKeySlot(''), 1);
  assert.equal(nextKeySlot('GOOGLE_API_KEY_1=abc\n'), 2);
  assert.equal(nextKeySlot('GOOGLE_API_KEY_1=abc\nGOOGLE_API_KEY_3=def\n'), 2);
  assert.equal(nextKeySlot('GOOGLE_API_KEY=legacy\n'), 2);
});

/* ---- discovery.js: Google model-list -> chat-model catalog diff ---- */

// Shaped like Google's real GET /v1beta/models response.
const googleModelsResponse = {
  models: [
    { name: 'models/gemini-3.8-flash', supportedGenerationMethods: ['generateContent', 'countTokens'] },
    { name: 'models/gemini-embedding-001', supportedGenerationMethods: ['embedContent'] },
    { name: 'models/gemini-3.8-flash-latest', supportedGenerationMethods: ['generateContent', 'countTokens'] },
    { name: 'models/gemini-3.8-flash-exp', supportedGenerationMethods: ['generateContent', 'countTokens'] },
    { name: 'models/gemini-3.8-pro', supportedGenerationMethods: ['generateContent', 'countTokens'] },
    { name: 'models/gemini-3.8-flash-tts', supportedGenerationMethods: ['generateContent'] },
    { name: 'models/gemini-3.5-transcribe', supportedGenerationMethods: ['generateContent'] },
    { name: 'models/gemini-3.5-flash-lite', supportedGenerationMethods: ['countTokens'] }, // no generateContent
  ],
};

test('filterChatModels: keeps real chat models, strips "models/", drops embeddings/-latest/non-chat/no-generateContent', () => {
  const ids = filterChatModels(googleModelsResponse.models);
  assert.deepEqual(ids, ['gemini-3.8-flash', 'gemini-3.8-flash-exp', 'gemini-3.8-pro']);
});

test('filterChatModels: handles a non-array input without throwing', () => {
  assert.deepEqual(filterChatModels(null), []);
  assert.deepEqual(filterChatModels(undefined), []);
});

test('sortByVersionDesc: newest version first, pro before flash before flash-lite within a version', () => {
  const ids = ['gemini-3.5-flash', 'gemini-3.8-flash-lite', 'gemini-3.8-pro', 'gemini-3.8-flash'];
  assert.deepEqual(sortByVersionDesc(ids), ['gemini-3.8-pro', 'gemini-3.8-flash', 'gemini-3.8-flash-lite', 'gemini-3.5-flash']);
});

test('sortByVersionDesc: unparseable versions sort last, stable for ties', () => {
  const ids = ['gemini-pro-vision', 'gemini-3.8-flash', 'gemini-nano'];
  assert.deepEqual(sortByVersionDesc(ids), ['gemini-3.8-flash', 'gemini-pro-vision', 'gemini-nano']);
});

test('diffCatalog: reports newly-discovered and no-longer-listed ids', () => {
  const previous = ['gemini-3.7-flash', 'gemini-3.8-flash'];
  const discovered = ['gemini-3.8-flash', 'gemini-3.8-pro'];
  assert.deepEqual(diffCatalog(previous, discovered), { added: ['gemini-3.8-pro'], removed: ['gemini-3.7-flash'] });
});

test('correctFinishReason: only rewrites stop, and only when tool calls are present', () => {
  const withTools = { finish_reason: 'stop' };
  correctFinishReason(withTools, [{ id: 'a' }]);
  assert.equal(withTools.finish_reason, 'tool_calls');

  const noTools = { finish_reason: 'stop' };
  correctFinishReason(noTools, []);
  assert.equal(noTools.finish_reason, 'stop');

  const truncated = { finish_reason: 'length' };
  correctFinishReason(truncated, [{ id: 'a' }]);
  assert.equal(truncated.finish_reason, 'length');
});
