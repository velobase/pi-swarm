/**
 * Zero-dependency SSE dashboard server.
 * Scheduler state comes from the supervisor (or `swarm_runtime` when standalone).
 * A job can extend it through optional hooks, all task-agnostic:
 *   progress(ctx)        -> { done, total, unit?, segments?: [{ label, value, tone? }] }
 *   dashboardPanels(ctx) -> [{ title, type: 'stats'|'table'|'bars'|'text', ... }]
 *   metricsMeta          -> { [metricName]: { label, unit } }
 *   alerts               -> thresholds, see DEFAULT_ALERTS
 */
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildProgress, evaluateHealth } from './state.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));

function normalizePanels(raw) {
  if (!Array.isArray(raw)) return [];
  return raw
    .filter(p => p && typeof p === 'object' && ['stats', 'table', 'bars', 'text'].includes(p.type))
    .slice(0, 12);
}

export function startDashboardServer({
  port = 3010,
  host = '127.0.0.1',
  runId,
  runtimeDb,
  getLiveInMemory = null,
  job = null,
}) {
  const hookCtx = { runtimeDb };
  const samples = []; // { t, done } for the rate calculation
  let jobProgress = null;
  let panels = [];
  let hookError = null;

  async function pollHooks() {
    try {
      if (typeof job?.progress === 'function') jobProgress = await job.progress(hookCtx);
      let next = [];
      if (typeof job?.dashboardCounters === 'function') {
        // Backwards compatible: a flat counters object becomes one stats panel.
        const counters = (await job.dashboardCounters(hookCtx)) || {};
        const items = Object.entries(counters).map(([label, value]) => ({ label, value }));
        if (items.length) next.push({ title: '', type: 'stats', items });
      }
      if (typeof job?.dashboardPanels === 'function') next = next.concat(normalizePanels(await job.dashboardPanels(hookCtx)));
      panels = next;
      hookError = null;
    } catch (err) {
      hookError = String(err.message || err).slice(0, 200);
    }
  }

  async function readScheduler() {
    if (typeof getLiveInMemory === 'function') {
      const mem = getLiveInMemory();
      if (mem) return mem;
    }
    const row = await runtimeDb.getRunDashboardState(runId);
    return { live: row?.snapshot || null, timeline: row?.timeline || [] };
  }

  async function buildState() {
    const { live, timeline } = await readScheduler();
    const now = Date.now();
    const progress = buildProgress({ jobProgress, live, samples, now });
    const timeoutMs = Number(live?.timeoutMs ?? job?.httpIdleTimeoutMs ?? 0);
    const health = evaluateHealth({ live, timeline, progress, timeoutMs, alerts: job?.alerts, now });
    return { live, timeline, progress, health, panels, metricsMeta: job?.metricsMeta || {}, timeoutMs, hookError };
  }

  async function sample() {
    await pollHooks();
    const { live } = await readScheduler().catch(() => ({ live: null }));
    const p = buildProgress({ jobProgress, live, samples: [], now: Date.now() });
    samples.push({ t: Date.now(), done: p.done });
    while (samples.length > 2 && Date.now() - samples[0].t > 10 * 60_000) samples.shift();
  }
  void sample();
  setInterval(() => void sample(), 5000).unref();

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || '/', 'http://localhost');
    try {
      if (url.pathname === '/api/state') {
        res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
        res.end(JSON.stringify(await buildState()));
        return;
      }
      if (url.pathname === '/api/task') {
        const taskId = url.searchParams.get('id');
        if (!taskId) {
          res.writeHead(400, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ error: 'Missing ?id=' }));
          return;
        }
        const info = await runtimeDb.inspectTask(runId, taskId);
        res.writeHead(info ? 200 : 404, { 'content-type': 'application/json', 'cache-control': 'no-store' });
        res.end(JSON.stringify(info || { error: 'Not found' }));
        return;
      }
      if (url.pathname === '/api/tasks') {
        const status = url.searchParams.get('status') || null;
        const limit = Number(url.searchParams.get('limit') || 50);
        const rows = await runtimeDb.listTasksByStatus(runId, status, limit);
        res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
        res.end(JSON.stringify({ tasks: rows }));
        return;
      }
      if (url.pathname === '/events') {
        res.writeHead(200, {
          'content-type': 'text/event-stream',
          'cache-control': 'no-store',
          connection: 'keep-alive',
          'x-accel-buffering': 'no',
        });
        const push = async () => {
          try {
            res.write(`data: ${JSON.stringify(await buildState())}\n\n`);
          } catch {}
        };
        void push();
        const timer = setInterval(push, 2000);
        req.on('close', () => clearInterval(timer));
        return;
      }
      if (url.pathname === '/' || url.pathname === '/index.html') {
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
        res.end(fs.readFileSync(path.join(here, 'index.html')));
        return;
      }
      res.writeHead(404).end('not found');
    } catch (err) {
      res.writeHead(500, { 'content-type': 'text/plain' }).end(String(err.message || err));
    }
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => resolve(server));
  });
}
