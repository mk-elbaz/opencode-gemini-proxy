/**
 * status.js — pure helpers for the event bus and in-band status messages,
 * split out of server.js so they're testable without booting the whole
 * proxy (which reads .env, validates keys, and binds a port on import).
 */

const PROXY_STATUS_PREFIX = '[gemini-proxy] ';

/* The proxy injects "[gemini-proxy] ..." lines into reasoning_content so
 * OpenCode shows live status in its "thinking" block. The adapter replays
 * that same reasoning_content back to us as history on the next turn — strip
 * our own lines back out so they don't get re-fed to the model as if the
 * assistant had "thought" them. Never touch `content`. Returns null when the
 * field should be deleted entirely (now empty/whitespace-only). */
export function stripProxyStatusLines(reasoningContent) {
  if (typeof reasoningContent !== 'string') return reasoningContent;
  const kept = reasoningContent
    .split('\n')
    .filter(line => !line.startsWith(PROXY_STATUS_PREFIX))
    .join('\n');
  return kept.trim() === '' ? null : kept;
}

/* Ring buffer push: keeps at most `max` most-recent entries, oldest first. */
export function pushRing(ring, evt, max) {
  ring.push(evt);
  if (ring.length > max) ring.shift();
  return ring;
}
