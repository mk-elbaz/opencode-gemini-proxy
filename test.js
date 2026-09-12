/**
 * test.js — self-check for the quota-classification logic in quota.js.
 * Run: node --test
 * No framework, no fixtures — just node:test + node:assert. Covers the exact
 * bug class this proxy already hit twice: per-model vs per-key scope, and
 * daily (RPD) vs short (RPM) quota detection.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { quotaScope, isDailyQuota, msUntilPacificMidnight } from './quota.js';
import { addToolCallIndices, correctFinishReason, patchStreamEvent } from './compat.js';

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
