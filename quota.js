/**
 * quota.js — pure quota-classification logic, split out of server.js so it's
 * testable without booting the whole proxy (which reads .env, validates keys,
 * and binds a port at import time).
 */

/* Quota scope: Google documents limits PER MODEL (RPM/TPM/RPD per model per
 * project), plus rarer project-wide caps ("worker local total request limit").
 * A per-model 429 must cool ONLY that model and rotate to the next model on
 * the SAME key. The key cools only on evidence of key/project-wide exhaustion:
 * worker-total messages, or quota details scoped per_project (not per_model). */
export const KEY_WIDE_RE = /worker local total|total request limit/i;

export function quotaScope(parsed, errMsg) {
  if (KEY_WIDE_RE.test(errMsg || '')) return 'key';
  try {
    const details = parsed?.error?.details || [];
    for (const d of details) {
      const blobs = [d.quotaMetric, d.quotaId,
        ...(d.violations || []).flatMap(v => [v.quotaMetric, v.quotaId, v.subject])];
      const text = blobs.filter(Boolean).join(' ');
      if (/per_model/i.test(text)) return 'model';
      if (/per_project/i.test(text)) return 'key';
    }
  } catch { /* fall through to default */ }
  return 'model'; // default: documented per-model quotas — rotate, don't park the key
}

/* Google's RPD (requests-per-day) quotas reset at UTC midnight and won't
 * recover within a short exponential cooldown — detect "PerDay"/"per_day" in
 * the quota details so the dashboard can show "daily limit" instead of a
 * misleading few-minutes countdown, and so the key parks until the actual
 * reset instead of being retried every few minutes for the rest of the day. */
export function isDailyQuota(parsed, errMsg) {
  if (/per[\s_]?day|requests per day/i.test(errMsg || '')) return true;
  try {
    const details = parsed?.error?.details || [];
    for (const d of details) {
      const blobs = [d.quotaMetric, d.quotaId,
        ...(d.violations || []).flatMap(v => [v.quotaMetric, v.quotaId, v.subject])];
      if (/per[\s_]?day/i.test(blobs.filter(Boolean).join(' '))) return true;
    }
  } catch { /* fall through */ }
  return false;
}

// Google resets Gemini API daily (RPD) quotas at midnight Pacific Time, NOT
// UTC — confirmed against Google's own rate-limit docs. Using Intl instead of
// a hardcoded UTC-7/-8 offset means PST/PDT (DST) is handled automatically by
// the platform's timezone database, with zero added dependencies.
const PACIFIC_TIME_FMT = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/Los_Angeles',
  hourCycle: 'h23',
  hour: '2-digit', minute: '2-digit', second: '2-digit',
});

export function msUntilPacificMidnight(now = new Date()) {
  const parts = Object.fromEntries(PACIFIC_TIME_FMT.formatToParts(now).map(p => [p.type, p.value]));
  const msSinceLocalMidnight =
    (Number(parts.hour) * 3600 + Number(parts.minute) * 60 + Number(parts.second)) * 1000 + now.getMilliseconds();
  return 24 * 60 * 60 * 1000 - msSinceLocalMidnight;
}

/* RPD (requests-per-day) quota is scoped PER KEY (per Google Cloud project),
 * not summed across the pool — so the count to learn from is the tripping
 * key's own count for the model, never the pooled total. Pure so it's
 * testable without touching usage.json/server state. */
export function learnedLimitFromTrip(countsForKey, model) {
  const n = countsForKey?.[model] || 0;
  return n > 0 ? n : null;
}

/* Which per-minute/per-day dimension a 429 belongs to, so the dashboard and
 * model_switch messages can say "tokens-per-minute" instead of a generic
 * "rate-limited". Same details-walking pattern as quotaScope/isDailyQuota. */
export function quotaDimension(parsed, errMsg) {
  try {
    const details = parsed?.error?.details || [];
    for (const d of details) {
      const blobs = [d.quotaMetric, d.quotaId,
        ...(d.violations || []).flatMap(v => [v.quotaMetric, v.quotaId, v.subject])];
      const text = blobs.filter(Boolean).join(' ');
      if (!text) continue;
      if (/PerDay/i.test(text)) return 'rpd';
      if (/PerMinute/i.test(text) && /token/i.test(text)) return 'tpm';
      if (/PerMinute/i.test(text) && /request/i.test(text)) return 'rpm';
    }
  } catch { /* fall through */ }
  if (/tokens per minute|input tokens|token count/i.test(errMsg || '')) return 'tpm';
  return 'unknown';
}

/* Google's 429 bodies carry the documented RetryInfo detail with the exact
 * wait time it wants ("37s", sometimes fractional "12.5s"), which is far more
 * accurate than our own exponential backoff guess. Falls back to the HTTP
 * Retry-After header (also seconds) when no RetryInfo detail is present.
 * Returns null when neither is available — caller keeps its own default. */
export function retryDelayMs(parsed, retryAfterHeader) {
  try {
    const details = parsed?.error?.details || [];
    for (const d of details) {
      if (d?.['@type'] === 'type.googleapis.com/google.rpc.RetryInfo' && d.retryDelay) {
        const secs = parseFloat(String(d.retryDelay).replace(/s$/i, ''));
        if (!Number.isNaN(secs)) return secs * 1000;
      }
    }
  } catch { /* fall through */ }
  if (retryAfterHeader != null) {
    const secs = Number(retryAfterHeader);
    if (!Number.isNaN(secs)) return secs * 1000;
  }
  return null;
}
