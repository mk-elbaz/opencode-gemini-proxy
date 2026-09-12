/**
 * compat.js — OpenAI-spec compliance fixes for Google's OpenAI-compatible
 * endpoint, split out of server.js so they're testable without booting the
 * whole proxy (which reads .env, validates keys, and binds a port on import).
 *
 * These are the only additions to an otherwise verbatim passthrough: they fill
 * in fields the OpenAI spec requires that Google's compat layer gets wrong,
 * and never touch anything that's already correct.
 */

/* Google's OpenAI-compat endpoint omits `tool_calls[].index` — the field a
 * spec-compliant client uses to tell "this chunk continues tool call N" from
 * "this is a new tool call" apart. */
export function addToolCallIndices(toolCalls) {
  if (!Array.isArray(toolCalls)) return;
  toolCalls.forEach((tc, i) => { if (typeof tc.index !== 'number') tc.index = i; });
}

/* A turn carrying tool calls must finish with "tool_calls", never "stop".
 * Google's non-streaming path gets this right; its streaming path does not.
 * Applied to every response path so all three agree regardless.
 * Only "stop" is corrected — "length"/"content_filter" mean what they say. */
export function correctFinishReason(choice, toolCalls) {
  if (choice?.finish_reason === 'stop' && Array.isArray(toolCalls) && toolCalls.length) {
    choice.finish_reason = 'tool_calls';
    return true;
  }
  return false;
}

/* Per-response fixes for a true SSE relay, where both problems above show up
 * at once and have to be tracked across chunks:
 *
 *  1. tool_calls[].index — Google emits one tool call per event rather than one
 *     array with all of them, so the index has to run as a counter across the
 *     whole response, not reset within each single-entry chunk.
 *
 *  2. finish_reason — Google closes a tool-calling stream with "stop". That
 *     field is how a client tells "the model is done" from "the model wants a
 *     tool run and this turn continues". OpenCode's TUI keys its per-message
 *     footer off exactly this (`finish && !["tool-calls","unknown"].includes(finish)`),
 *     so a wrong "stop" makes it treat every single tool call as a completed
 *     turn and redraw the agent/model/duration header for each one.
 *
 * `state` is created once per response: { nextIndex: 0, sawToolCall: false }.
 * Returns the original payload string untouched when there's nothing to fix,
 * so unaffected events stay byte-for-byte verbatim.
 */
export function patchStreamEvent(payload, state) {
  let evt;
  try { evt = JSON.parse(payload); } catch { return payload; }
  const choice = evt?.choices?.[0];
  if (!choice) return payload;
  let changed = false;

  const tcs = choice.delta?.tool_calls;
  if (Array.isArray(tcs) && tcs.length) {
    state.sawToolCall = true;
    for (const tc of tcs) {
      if (typeof tc.index !== 'number') { tc.index = state.nextIndex++; changed = true; }
      else state.nextIndex = Math.max(state.nextIndex, tc.index + 1);
    }
  }

  if (state.sawToolCall && choice.finish_reason === 'stop') {
    choice.finish_reason = 'tool_calls';
    changed = true;
  }

  return changed ? JSON.stringify(evt) : payload;
}
