/**
 * Swarm Supervisor: manages worker processes, AIMD LLM Gate, external tool SlotPools,
 * Unix control socket (`steer` / `set_limit` / `status` / `stop`), and PostgreSQL runtime state.
 */
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fork } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Gate, SlotPool } from './gate.mjs';
import { RuntimeDb } from './runtime-db.mjs';
import { startDashboardServer } from './dashboard/server.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const redact = s => String(s || '').replace(/sk-[A-Za-z0-9_-]+/g, '[REDACTED]').slice(0, 600);

export function defaultSocketPath(runId) {
  const safe = String(runId).replace(/[^a-zA-Z0-9._-]+/g, '_');
  return `/tmp/pi-swarm-${safe}.sock`;
}

export async function runSupervisor({
  jobPath,
  runtimeDatabaseUrl,
  concurrency,
  workers: workersCount,
  socketPath,
  dashboardPort = null,
  dashboardHost = '127.0.0.1',
  onlyTasks = null,
  exitWhenDone = true,
}) {
  const absJobPath = path.resolve(jobPath);
  const jobMod = await import(pathToFileURL(absJobPath).href);
  const job = typeof jobMod.default === 'function' ? await jobMod.default() : jobMod.default;

  const dbUrl = runtimeDatabaseUrl || process.env.SWARM_DATABASE_URL || job.runtimeDatabaseUrl;
  const runtimeDb = new RuntimeDb(dbUrl, {
    max: 15,
    applicationName: `pi-swarm-supervisor-${job.runId}`,
  });
  await runtimeDb.migrate();

  const rawTasks = Array.isArray(job.tasks) ? job.tasks : await job.tasks({ runtimeDb });
  const normalizedTasks = rawTasks.map(t =>
    typeof t === 'string' ? { id: t, payload: { id: t } } : { id: String(t.id), payload: t.payload ?? t }
  );

  const defaultPoolsCfg = job.pools || {};
  const restored = await runtimeDb.initRun({
    runId: job.runId,
    name: job.name || job.runId,
    config: {
      concurrency: concurrency || job.concurrency,
      workers: workersCount || job.workers,
      model: { provider: job.model.provider, id: job.model.id },
    },
    tasks: normalizedTasks,
    defaultGateLimit: job.gate?.limit || 300,
    defaultPools: defaultPoolsCfg,
  });

  const capacity = Number(concurrency || job.concurrency || 3000);
  const shardCount = Number(workersCount || job.workers || 30);
  const gateCfg = { ...(job.gate || {}) };
  if (Number.isFinite(restored.gateLimit) && restored.gateLimit > 0) {
    gateCfg.limit = restored.gateLimit;
  }
  const gate = new Gate(gateCfg);

  const slotPools = {};
  for (const [poolName, cfg] of Object.entries(defaultPoolsCfg)) {
    const savedPool = restored.pools?.[poolName] || {};
    const lim = Number(savedPool.limit || cfg.limit || cfg.max || 120);
    const rpm = Number(savedPool.perMinute ?? cfg.perMinute ?? cfg.rpm ?? 0);
    slotPools[poolName] = new SlotPool(lim, rpm);
  }

  const dbTasks = await runtimeDb.loadTasks(job.runId);
  const filterSet = onlyTasks ? new Set(onlyTasks) : null;
  const activeTasks = dbTasks.filter(t => !filterSet || filterSet.has(t.id));
  const byId = new Map(activeTasks.map(t => [t.id, { id: t.id, payload: t.payload }]));

  const states = {};
  const queue = [];
  for (const row of activeTasks) {
    states[row.id] = {
      task: row.id,
      status: row.status === 'complete' ? 'complete' : 'pending',
      attempt: Number(row.attempt || 0),
      worker: null,
      lastError: row.last_error || null,
      consumedBroadcasts: new Set(),
    };
    if (states[row.id].status === 'pending') queue.push(row.id);
  }

  const startedMs = Date.now();
  const RUN = {
    id: job.runId,
    name: job.name || job.runId,
    tasks: activeTasks.length,
    workers: shardCount,
    startedAt: new Date(restored.startedAt || startedMs).toISOString(),
  };

  const workers = [];
  const recentFailures = [];
  const waitSamples = [];
  const broadcasts = new Map();
  const pendingSteerWaiters = new Map();
  const timeline = Array.isArray(restored.timeline) ? restored.timeline.slice(-720) : [];

  let stopping = false;
  let finishedAt = null;
  let piRetries = 0;
  let piContinuations = 0;
  let toolSuccess = 0;
  let toolErrors = 0;
  let resumedSessions = 0;
  let jinaCalls = restored.jinaCalls || 0;
  let jinaTokens = restored.jinaTokens || 0;
  let jina429 = 0;
  let latestSnapshot = null;

  function recordError(taskId, text) {
    const e = { id: taskId, at: new Date().toISOString(), error: redact(text) };
    recentFailures.push(e);
    if (recentFailures.length > 25) recentFailures.shift();
  }

  function pendingBroadcastsFor(taskId) {
    const s = states[taskId];
    const out = [];
    for (const b of broadcasts.values()) {
      if (b.tasks && !b.tasks.includes(taskId)) continue;
      if (!s.consumedBroadcasts.has(b.id)) out.push({ id: b.id, text: b.text });
    }
    return out;
  }

  function fill() {
    if (stopping) return;
    const maxPerWorker = Math.ceil(capacity / shardCount);
    while (queue.length) {
      const active = workers.reduce((n, w) => n + w.tasks.size, 0);
      if (active >= capacity) return;
      const available = workers
        .filter(w => w.alive && w.ready && w.tasks.size < maxPerWorker)
        .sort((a, b) => a.tasks.size - b.tasks.size);
      if (!available.length) return;
      const id = queue.shift();
      const s = states[id];
      if (!s || s.status === 'complete' || s.status === 'running') continue;
      const w = available[0];
      s.status = 'running';
      s.attempt = (s.attempt || 0) + 1;
      s.worker = w.index;
      w.tasks.add(id);
      void runtimeDb.markTaskRunning(job.runId, id, w.index, s.attempt).catch(() => {});
      w.child.send({
        type: 'start',
        task: byId.get(id),
        attempt: s.attempt,
        broadcasts: pendingBroadcastsFor(id),
      });
    }
  }

  async function checkAllComplete() {
    const all = Object.values(states);
    if (all.length > 0 && all.every(s => s.status === 'complete') && !finishedAt) {
      finishedAt = new Date().toISOString();
      await persistHeartbeat(true);
      if (exitWhenDone) {
        await shutdown(0);
      }
    }
  }

  async function finish(w, id, message) {
    const s = states[id];
    if (!s || s.status !== 'running') return;
    w.tasks.delete(id);
    if (message.continuations) piContinuations += Number(message.continuations) || 0;
    if (message.error) {
      s.lastError = redact(message.error);
      recordError(id, message.error);
      s.status = 'backoff';
      const backoffMs = Math.min(60_000, 4000 * Math.pow(1.5, Math.min(8, s.attempt - 1)));
      s.retryAt = Date.now() + backoffMs;
      await runtimeDb.markTaskBackoff(job.runId, id, s.lastError).catch(() => {});
    } else {
      s.status = 'complete';
      await runtimeDb
        .markTaskComplete(job.runId, id, { retainSession: Boolean(job.retainSessions) })
        .catch(() => {});
      await checkAllComplete();
    }
    fill();
  }

  function onMessage(w, m) {
    if (m.type === 'llm_acquire') {
      gate.acquire(
        m.id,
        w.index,
        () => w.child.connected && w.child.send({ type: 'llm_grant', id: m.id }),
        { priority: m.priority || 0 }
      );
      return;
    }
    if (m.type === 'llm_release') {
      gate.release(m.id, m.status);
      if (Number.isFinite(m.waitMs)) waitSamples.push(m.waitMs);
      return;
    }
    if (m.type === 'slot_acquire') {
      const sp = slotPools[m.pool];
      if (sp) {
        sp.acquire(
          m.id,
          w.index,
          () => w.child.connected && w.child.send({ type: 'slot_grant', pool: m.pool, id: m.id })
        );
      } else if (w.child.connected) {
        w.child.send({ type: 'slot_grant', pool: m.pool, id: m.id });
      }
      return;
    }
    if (m.type === 'slot_release') {
      slotPools[m.pool]?.release(m.id);
      return;
    }
    if (m.type === 'jina_usage') {
      jinaCalls++;
      jinaTokens += Number(m.tokens || 0);
      if (m.status === 429) jina429++;
      return;
    }
    if (m.type === 'ready') {
      w.ready = true;
      fill();
      return;
    }
    if (m.type === 'session_ready') {
      if (m.resumed) resumedSessions++;
      return;
    }
    if (m.type === 'steer_ack') {
      const waiter = pendingSteerWaiters.get(m.id);
      if (waiter) {
        waiter.responses.push(m);
        if (waiter.responses.length >= waiter.expected) {
          clearTimeout(waiter.timer);
          pendingSteerWaiters.delete(m.id);
          waiter.resolve(waiter.responses);
        }
      }
      return;
    }
    if (m.type === 'broadcast_consumed') {
      const b = broadcasts.get(m.id);
      if (b) b.consumedBy.add(m.task);
      const s = states[m.task];
      if (s) s.consumedBroadcasts.add(m.id);
      void runtimeDb.recordBroadcastConsumed(job.runId, m.id, m.task).catch(() => {});
      return;
    }
    if (m.type === 'retry') {
      piRetries++;
    } else if (m.type === 'tool_end') {
      if (m.isError) {
        toolErrors++;
        if (m.error) recordError(m.task, `${m.tool}: ${m.error}`);
      } else {
        toolSuccess++;
      }
    } else if (m.type === 'run_finished') {
      void finish(w, m.task, m);
    }
  }

  function spawnWorker(index) {
    const child = fork(path.join(here, 'worker.mjs'), [], {
      env: {
        ...process.env,
        PI_SHARD: String(index),
        PI_SWARM_JOB: absJobPath,
        SWARM_DATABASE_URL: dbUrl,
      },
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    });
    const w = { index, child, alive: true, ready: false, tasks: new Set() };
    child.on('message', m => onMessage(w, m));
    child.on('exit', () => {
      w.alive = false;
      w.ready = false;
      gate.dropWorker(index);
      for (const sp of Object.values(slotPools)) sp.dropWorker(index);
      for (const id of w.tasks) {
        const s = states[id];
        if (s && s.status === 'running') {
          s.status = 'backoff';
          s.retryAt = Date.now() + 2000;
        }
      }
      w.tasks.clear();
      if (!stopping) {
        workers[index] = spawnWorker(index);
      }
    });
    return w;
  }

  for (let i = 0; i < shardCount; i++) {
    workers.push(spawnWorker(i));
  }

  // Check if all tasks were already complete before starting
  await checkAllComplete();
  if (stopping) return;

  const sock = socketPath || defaultSocketPath(job.runId);
  try {
    fs.unlinkSync(sock);
  } catch {}

  const controlServer = net.createServer(conn => {
    let buf = '';
    conn.on('data', chunk => {
      buf += chunk.toString('utf8');
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!line) continue;
        void handleControl(conn, line);
      }
    });
  });
  controlServer.listen(sock);

  async function handleControl(conn, line) {
    try {
      const cmd = JSON.parse(line);
      if (cmd.action === 'status') {
        conn.write(JSON.stringify({ ok: true, live: buildLiveObject() }) + '\n');
        conn.end();
        return;
      }
      if (cmd.action === 'set_limit') {
        if (cmd.pool === 'gate') {
          gate.setLimits({ limit: cmd.limit, min: cmd.min, max: cmd.max });
          conn.write(JSON.stringify({ ok: true, gate: gate.snapshot(false) }) + '\n');
        } else if (slotPools[cmd.pool]) {
          slotPools[cmd.pool].setLimits({ limit: cmd.limit, perMinute: cmd.perMinute });
          conn.write(
            JSON.stringify({ ok: true, pool: cmd.pool, ...slotPools[cmd.pool].snapshot() }) + '\n'
          );
        } else {
          conn.write(JSON.stringify({ ok: false, error: `Unknown pool: ${cmd.pool}` }) + '\n');
        }
        conn.end();
        return;
      }
      if (cmd.action === 'steer') {
        const id = cmd.id || 'bc-' + Date.now();
        const entry = {
          id,
          text: String(cmd.text || ''),
          interrupt: Boolean(cmd.interrupt),
          tasks: Array.isArray(cmd.tasks) && cmd.tasks.length ? cmd.tasks : null,
          at: new Date().toISOString(),
          consumedBy: new Set(),
        };
        broadcasts.set(id, entry);
        await runtimeDb.saveBroadcast(job.runId, entry).catch(() => {});
        const aliveWorkers = workers.filter(w => w.alive && w.ready);
        const acks = await new Promise(resolve => {
          if (!aliveWorkers.length) return resolve([]);
          const timer = setTimeout(() => {
            const w = pendingSteerWaiters.get(id);
            pendingSteerWaiters.delete(id);
            resolve(w ? w.responses : []);
          }, 10_000);
          pendingSteerWaiters.set(id, {
            expected: aliveWorkers.length,
            responses: [],
            resolve,
            timer,
          });
          for (const w of aliveWorkers) {
            w.child.send({
              type: 'steer',
              id,
              text: entry.text,
              interrupt: entry.interrupt,
              tasks: entry.tasks,
            });
          }
        });
        conn.write(
          JSON.stringify({
            ok: true,
            id,
            workersAcked: acks.length,
            matched: acks.reduce((n, a) => n + (a.matched || 0), 0),
            queuedSteer: acks.reduce((n, a) => n + (a.queuedSteer || 0), 0),
            interruptedAndSteered: acks.reduce((n, a) => n + (a.interruptedAndSteered || 0), 0),
            startedImmediately: acks.reduce((n, a) => n + (a.startedImmediately || 0), 0),
          }) + '\n'
        );
        conn.end();
        return;
      }
      if (cmd.action === 'stop') {
        conn.write(JSON.stringify({ ok: true, stopping: true }) + '\n');
        conn.end();
        await shutdown(0);
        return;
      }
      conn.write(JSON.stringify({ ok: false, error: `Unknown action: ${cmd.action}` }) + '\n');
      conn.end();
    } catch (err) {
      conn.write(JSON.stringify({ ok: false, error: err.message }) + '\n');
      conn.end();
    }
  }

  const pct = (arr, p) => {
    if (!arr.length) return 0;
    const s = [...arr].sort((a, b) => a - b);
    return s[Math.min(s.length - 1, Math.floor(s.length * p))];
  };

  function buildPoolsSnapshot() {
    const out = {};
    for (const [k, sp] of Object.entries(slotPools)) {
      out[k] =
        k === 'jina'
          ? { ...sp.snapshot(), calls: jinaCalls, tokens: jinaTokens, r429: jina429 }
          : sp.snapshot();
    }
    return out;
  }

  function buildLiveObject() {
    const all = Object.values(states);
    const done = all.filter(s => s.status === 'complete').length;
    const active = all.filter(s => s.status === 'running').length;
    const backoff = all.filter(s => s.status === 'backoff').length;
    return {
      run: RUN,
      at: new Date().toISOString(),
      finishedAt,
      elapsedSec: Math.round(((finishedAt ? Date.parse(finishedAt) : Date.now()) - startedMs) / 1000),
      resumedSessions,
      tasks: {
        total: all.length,
        done,
        ok: done,
        failed: backoff,
        active,
      },
      gate: gate.snapshot(false),
      pools: buildPoolsSnapshot(),
      pi: { retries: piRetries, continuations: piContinuations },
      counters: { toolSuccess, toolErrors },
      recentFailures,
    };
  }

  async function persistHeartbeat(withTimeline = false) {
    latestSnapshot = buildLiveObject();
    await runtimeDb
      .saveHeartbeat(job.runId, {
        gateLimit: gate.limit,
        pools: buildPoolsSnapshot(),
        jinaCalls,
        jinaTokens,
        snapshot: latestSnapshot,
        timeline: withTimeline ? timeline : null,
        finishedAt,
      })
      .catch(() => {});
  }

  let dashServer = null;
  const effectiveDashPort = dashboardPort ?? job.dashboardPort ?? null;
  if (effectiveDashPort) {
    dashServer = await startDashboardServer({
      port: Number(effectiveDashPort),
      host: dashboardHost,
      runId: job.runId,
      runtimeDb,
      getLiveInMemory: () => ({ live: latestSnapshot || buildLiveObject(), timeline }),
      dashboardCounters: job.dashboardCounters,
    });
    console.log(`pi-swarm dashboard listening on http://${dashboardHost}:${effectiveDashPort}`);
  }

  const liveTimer = setInterval(() => {
    const now = Date.now();
    for (const s of Object.values(states)) {
      if (s.status === 'backoff' && now >= (s.retryAt || 0)) {
        s.status = 'pending';
        queue.push(s.task);
      }
    }
    fill();
    void persistHeartbeat(false);
  }, 2000);

  const tickTimer = setInterval(() => {
    const s = gate.snapshot(true);
    const all = Object.values(states);
    const done = all.filter(x => x.status === 'complete').length;
    timeline.push({
      at: new Date().toISOString(),
      t: Math.round((Date.now() - startedMs) / 1000),
      ...s,
      done,
      waitP50: pct(waitSamples, 0.5),
      waitP95: pct(waitSamples, 0.95),
    });
    if (timeline.length > 720) timeline.shift();
    waitSamples.length = 0;
    void persistHeartbeat(true);
  }, 10_000);

  await persistHeartbeat(false);

  async function shutdown(code = 0) {
    if (stopping) return;
    stopping = true;
    clearInterval(liveTimer);
    clearInterval(tickTimer);
    gate.close();
    for (const sp of Object.values(slotPools)) sp.close();
    try {
      controlServer.close();
      fs.unlinkSync(sock);
    } catch {}
    if (dashServer) {
      dashServer.close();
    }
    await Promise.allSettled(
      workers.map(
        w =>
          new Promise(resolve => {
            if (!w.alive) return resolve();
            const timer = setTimeout(() => {
              w.child.kill('SIGKILL');
              resolve();
            }, 8000);
            w.child.once('exit', () => {
              clearTimeout(timer);
              resolve();
            });
            w.child.send({ type: 'stop' });
          })
      )
    );
    await persistHeartbeat(true);
    await runtimeDb.close();
    process.exit(code);
  }

  process.on('SIGTERM', () => void shutdown(0));
  process.on('SIGINT', () => void shutdown(0));
}
