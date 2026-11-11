/**
 * Pure helpers behind the dashboard: progress rate, health verdict, error grouping.
 * No I/O here so they are easy to test.
 */

export const DEFAULT_ALERTS = {
  queueWaitWarnRatio: 0.5, // queue wait p95 / request timeout
  queueWaitCritRatio: 0.8,
  backoffWarnShare: 0.3, // share of 429/503 among recent upstream results
  netWarnShare: 0.5, // share of aborted/network failures
  errorCritShare: 0.5, // share of other 4xx/5xx (auth storms etc.)
  staleSec: 20,
};

/** Collapse an error message into a stable group key (drops quoted values and numbers). */
export function errorKey(text) {
  const raw = String(text ?? '');
  const tool = /^([A-Za-z_][\w-]*): /.exec(raw)?.[1];
  let msg = tool ? raw.slice(tool.length + 2) : raw;
  const inner = /"text":"((?:[^"\\]|\\.)*)"/.exec(msg);
  if (inner) msg = inner[1];
  msg = msg
    .replace(/\\n[\s\S]*$/, '') // keep the first line of multi-line tool errors
    .replace(/\\"(?:[^"\\]|\\[^"])*\\"/g, '"…"')
    .replace(/"[^"]*"/g, '"…"')
    .replace(/\d+/g, 'N')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 100);
  return (tool ? tool + ': ' : '') + (msg || 'unknown error');
}

/** Items per minute over the trailing window, or null when there is not enough history. */
export function computeRate(samples, now = Date.now(), windowMs = 5 * 60_000) {
  if (!Array.isArray(samples) || samples.length < 2) return null;
  const last = samples[samples.length - 1];
  const ref = samples.find(s => now - s.t <= windowMs) || samples[0];
  const spanMs = last.t - ref.t;
  if (spanMs < 30_000) return null;
  return Math.max(0, (last.done - ref.done) / (spanMs / 60_000));
}

/**
 * Progress shown at the top. Uses the job's own numbers when it provides a
 * `progress()` hook; otherwise falls back to task counts, ignoring tasks that
 * were already complete when the run started.
 */
export function buildProgress({ jobProgress, live, samples, now = Date.now() }) {
  let done, total, unit, segments, source;
  if (jobProgress && Number.isFinite(Number(jobProgress.total))) {
    done = Number(jobProgress.done) || 0;
    total = Number(jobProgress.total) || 0;
    unit = jobProgress.unit || '';
    segments = Array.isArray(jobProgress.segments) ? jobProgress.segments : null;
    source = 'job';
  } else {
    const t = live?.tasks || {};
    const skipped = Number(t.skipped) || 0;
    done = Math.max(0, (Number(t.done) || 0) - skipped);
    total = Math.max(0, (Number(t.total) || 0) - skipped);
    unit = 'tasks';
    segments = null;
    source = 'tasks';
  }
  const ratePerMin = computeRate(samples, now);
  const remaining = Math.max(0, total - done);
  const etaSec = ratePerMin && ratePerMin > 0 && remaining > 0 ? Math.round((remaining / ratePerMin) * 60) : null;
  return { done, total, remaining, unit, segments, source, ratePerMin, etaSec };
}

/** One verdict for the whole run: { level: 'ok'|'warn'|'crit'|'done'|'idle', code, params }. */
export function evaluateHealth({ live, timeline = [], progress, timeoutMs, alerts = {}, now = Date.now() }) {
  const a = { ...DEFAULT_ALERTS, ...alerts };
  if (!live || !live.tasks) return { level: 'idle', code: 'idle', params: {} };
  if (live.finishedAt) return { level: 'done', code: 'finished', params: {} };

  const ageSec = Math.round((now - Date.parse(live.at)) / 1000);
  if (ageSec > a.staleSec) return { level: 'crit', code: 'stale', params: { ageSec } };

  const recent = timeline.slice(-6);
  const sum = recent.reduce(
    (acc, r) => ({
      ok: acc.ok + (r.window?.ok || 0),
      backoff: acc.backoff + (r.window?.backoff || 0),
      other: acc.other + (r.window?.other || 0),
      net: acc.net + (r.window?.net || 0),
    }),
    { ok: 0, backoff: 0, other: 0, net: 0 }
  );
  const all = sum.ok + sum.backoff + sum.other + sum.net;
  const last = recent[recent.length - 1] || {};
  const waitP95 = Number(last.waitP95) || 0;
  const rate = progress?.ratePerMin ?? null;

  if (all >= 20 && sum.other / all >= a.errorCritShare) {
    return { level: 'crit', code: 'upstream_errors', params: { share: sum.other / all, count: sum.other } };
  }
  if (timeoutMs > 0 && waitP95 >= timeoutMs * a.queueWaitCritRatio) {
    return { level: 'crit', code: 'queue_wait', params: { waitSec: waitP95 / 1000, timeoutSec: timeoutMs / 1000 } };
  }
  if (timeoutMs > 0 && waitP95 >= timeoutMs * a.queueWaitWarnRatio) {
    return { level: 'warn', code: 'queue_wait', params: { waitSec: waitP95 / 1000, timeoutSec: timeoutMs / 1000 } };
  }
  if (all >= 20 && sum.net / all >= a.netWarnShare) {
    return { level: 'warn', code: 'network_failures', params: { share: sum.net / all, count: sum.net } };
  }
  if (all >= 20 && sum.backoff / all >= a.backoffWarnShare) {
    return { level: 'warn', code: 'rate_limited', params: { share: sum.backoff / all, count: sum.backoff } };
  }
  if (rate === 0 && (live.tasks.active || 0) > 0) {
    return { level: 'warn', code: 'no_progress', params: {} };
  }
  return { level: 'ok', code: 'healthy', params: { ratePerMin: rate } };
}
