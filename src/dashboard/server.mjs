/**
 * Zero-dependency SSE telemetry dashboard reading from `swarm_runtime` PostgreSQL
 * (plus an optional `dashboardCounters()` hook on the job definition).
 */
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const htmlTemplate = fs.readFileSync(path.join(here, 'index.html'), 'utf8');

export function startDashboardServer({
  port = 3010,
  host = '127.0.0.1',
  runId,
  runtimeDb,
  getLiveInMemory = null,
  dashboardCounters = null,
}) {
  let latestCustom = {};

  if (typeof dashboardCounters === 'function') {
    const pollCustom = async () => {
      try {
        latestCustom = (await dashboardCounters({ runtimeDb })) || {};
      } catch {}
    };
    void pollCustom();
    setInterval(pollCustom, 4000).unref();
  }

  async function buildState() {
    if (typeof getLiveInMemory === 'function') {
      const mem = getLiveInMemory();
      if (mem) return { ...mem, custom: latestCustom };
    }
    const row = await runtimeDb.getRunDashboardState(runId);
    return {
      live: row?.snapshot || null,
      timeline: row?.timeline || [],
      custom: latestCustom,
    };
  }

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || '/', 'http://localhost');
    if (url.pathname === '/api/state') {
      const payload = await buildState();
      res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(JSON.stringify(payload));
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
          const payload = await buildState();
          res.write(`data: ${JSON.stringify(payload)}\n\n`);
        } catch {}
      };
      void push();
      const timer = setInterval(push, 2000);
      req.on('close', () => clearInterval(timer));
      return;
    }
    if (url.pathname === '/' || url.pathname === '/index.html') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
      res.end(htmlTemplate);
      return;
    }
    res.writeHead(404).end('not found');
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => resolve(server));
  });
}
