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
