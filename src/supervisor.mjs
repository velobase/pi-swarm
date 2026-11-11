/**
 * Swarm Supervisor: manages worker processes, AIMD+TPM LLM Gate, SlotPools,
 * demand-driven session admission, circuit breaker, budget cap, stuck-session recovery,
 * sample dry-runs, webhook alerts, control socket, and PostgreSQL state persistence.
 */
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fork } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Gate, SlotPool } from './gate.mjs';
import { RuntimeDb } from './runtime-db.mjs';
import { startDashboardServer } from './dashboard/server.mjs';
import { errorKey } from './dashboard/state.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const redact = s => String(s || '').replace(/sk-[A-Za-z0-9_-]+/g, '[REDACTED]').slice(0, 600);

export function defaultSocketPath(runId) {
  const safe = String(runId).replace(/[^a-zA-Z0-9._-]+/g, '_');
  return `/tmp/pi-swarm-${safe}.sock`;
}

function computeTurnCostUsd(usage, costSpec) {
  if (!costSpec || typeof costSpec !== 'object') return 0;
  const inp = (Number(usage.input) || 0) * (Number(costSpec.input) || 0);
  const out = (Number(usage.output) || 0) * (Number(costSpec.output) || 0);
  const cr = (Number(usage.cacheRead) || 0) * (Number(costSpec.cacheRead) || 0);
  const cw = (Number(usage.cacheWrite) || 0) * (Number(costSpec.cacheWrite) || 0);
  return (inp + out + cr + cw) / 1_000_000;
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
  sampleCount = null,
  budgetUsd: budgetOverride = undefined,
  webhookUrl: webhookOverride = undefined,
  exitWhenDone = true,
  exitProcess = true,
}) {
  const absJobPath = path.resolve(jobPath);
  const jobMod = await import(pathToFileURL(absJobPath).href);
  const job = typeof jobMod.default === 'function' ? await jobMod.default() : jobMod.default;

  const dbUrl = runtimeDatabaseUrl || process.env.SWARM_DATABASE_URL || job.runtimeDatabaseUrl;
  const runtimeDb = new RuntimeDb(dbUrl, {
    max: 15,
    applicationName: `pi-swarm-supervisor-${job.runId}`,
  });
  let rawTasks;
  let restored;
  const defaultPoolsCfg = job.pools || {};
  const initialBudget =
    budgetOverride !== undefined
      ? Number(budgetOverride)
      : job.budgetUsd !== null && job.budgetUsd !== undefined
        ? Number(job.budgetUsd)
        : null;
  try {
    await runtimeDb.migrate();
    rawTasks = Array.isArray(job.tasks) ? job.tasks : await job.tasks({ runtimeDb });
    const normalizedTasks = rawTasks.map(t =>
      typeof t === 'string' ? { id: t, payload: { id: t } } : { id: String(t.id), payload: t.payload ?? t }
    );
    restored = await runtimeDb.initRun({
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
      defaultBudgetUsd: Number.isFinite(initialBudget) ? initialBudget : null,
    });
  } catch (err) {
    await runtimeDb.close();
    throw err;
  }

  const capacity = Number(concurrency || job.concurrency || 3000);
  const shardCount = Number(workersCount || job.workers || 30);
  const gateCfg = { ...(job.gate || {}) };
  if (Number.isFinite(restored.gateLimit) && restored.gateLimit > 0) {
    gateCfg.limit = Math.min(gateCfg.max || 1500, Math.max(gateCfg.min || 1, restored.gateLimit));
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
  let activeTasks = dbTasks;
  if (onlyTasks && onlyTasks.length > 0) {
    const filterSet = new Set(onlyTasks.map(String));
    activeTasks = dbTasks.filter(t => filterSet.has(t.id));
  } else if (sampleCount && Number(sampleCount) > 0) {
    const pendingRows = dbTasks.filter(t => t.status === 'pending');
    // Spread sample evenly across the task list for representative coverage
    const n = Math.min(Number(sampleCount), pendingRows.length);
    const picked = [];
    for (let i = 0; i < n; i++) {
      const idx = Math.floor((i * pendingRows.length) / n);
      picked.push(pendingRows[idx]);
    }
    activeTasks = picked;
  }

  const byId = new Map(activeTasks.map(t => [t.id, { id: t.id, payload: t.payload }]));
  const maxFailures = Number(job.taskRetries?.maxFailures ?? 5);
  const baseBackoffMs = Number(job.taskRetries?.baseDelayMs ?? 4000);
  const maxBackoffMs = Number(job.taskRetries?.maxDelayMs ?? 60_000);
  const stallTimeoutMs = Number(job.stallTimeoutMs ?? 10 * 60_000);
  const admissionCfg = job.sessionAdmission || { enabled: true, perGateSlot: 3, minActive: 100 };
  const breakerCfg = job.circuitBreaker || { enabled: true, minRequests: 20, fatalShare: 0.5 };
  const webhookUrl = webhookOverride ?? process.env.PI_SWARM_WEBHOOK_URL ?? job.webhookUrl ?? null;

  const states = {};
  const queue = [];
  for (const row of activeTasks) {
    const st =
      row.status === 'complete'
        ? 'complete'
        : row.status === 'failed'
          ? 'failed'
          : 'pending';
    states[row.id] = {
      task: row.id,
      status: st,
      attempt: Number(row.attempt || 0),
      failures: Number(row.failures || 0),
      worker: null,
      startedAt: null,
      lastActivityAt: null,
      lastError: row.last_error || null,
      consumedBroadcasts: new Set(),
    };
    if (st === 'pending') queue.push(row.id);
  }

  let skipped = Object.values(states).filter(s => s.status === 'complete').length;

  const startedMs = Date.now();
  const RUN = {
    id: job.runId,
    name: job.name || job.runId,
    tasks: activeTasks.length,
    catalogTotalTasks: dbTasks.length,
    sampleMode: Boolean(sampleCount && Number(sampleCount) > 0),
    workers: shardCount,
    startedAt: new Date(restored.startedAt || startedMs).toISOString(),
  };

  const workers = [];
  const recentFailures = [];
  const waitSamples = [];
  const broadcasts = new Map();
  const pendingSteerWaiters = new Map();
  const timeline = Array.isArray(restored.timeline) ? restored.timeline.slice(-720) : [];
  const endpointStats = {}; // endpointName -> { ok, backoff, other }

  let stopping = false;
  let paused = false;
  let pauseReason = null;
  let finishedAt = null;
  let budgetUsd = restored.budgetUsd;
  let piRetries = 0;
  let piContinuations = 0;
  let stalledAborts = 0;
  let toolSuccess = 0;
  let toolErrors = 0;
  let resumedSessions = 0;
  const metrics = { ...(restored.metrics || {}) };
  const initialMetrics = { ...metrics };
  const usage = {
    inputTokens: Number(restored.usage?.inputTokens || 0),
    outputTokens: Number(restored.usage?.outputTokens || 0),
    cacheReadTokens: Number(restored.usage?.cacheReadTokens || 0),
    cacheWriteTokens: Number(restored.usage?.cacheWriteTokens || 0),
    costUsd: Number(restored.usage?.costUsd || 0),
  };
  const initialUsage = { ...usage };
  let latestSnapshot = null;
  let lastWebhookEvent = null;

  let resolveDone;
  const donePromise = new Promise(resolve => {
    resolveDone = resolve;
  });

  async function notifyWebhook(eventType, summaryText, extra = {}) {
    if (!webhookUrl || lastWebhookEvent === `${eventType}:${pauseReason || ''}`) return;
    lastWebhookEvent = `${eventType}:${pauseReason || ''}`;
    try {
      await fetch(webhookUrl, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          text: `[pi-swarm · ${RUN.name}] ${summaryText}`,
          runId: RUN.id,
          event: eventType,
          at: new Date().toISOString(),
          ...extra,
        }),
      });
    } catch {}
  }

  function pauseSwarm(reason, eventType = 'paused') {
    if (paused) return;
    paused = true;
    pauseReason = String(reason || 'manual');
    gate.pause(pauseReason);
    void persistHeartbeat(false);
    void notifyWebhook(eventType, `Swarm paused (${pauseReason})`, { reason: pauseReason });
  }

  function resumeSwarm() {
    paused = false;
    pauseReason = null;
    gate.resume();
    fill();
    void persistHeartbeat(false);
    void notifyWebhook('resumed', 'Swarm resumed');
  }

  const errorGroups = new Map();
  function recordError(taskId, text) {
    const e = { id: taskId, at: new Date().toISOString(), error: redact(text) };
    recentFailures.push(e);
    if (recentFailures.length > 25) recentFailures.shift();
    const key = errorKey(e.error);
    const g = errorGroups.get(key) || { count: 0 };
    errorGroups.set(key, { count: g.count + 1, lastAt: e.at, lastTask: taskId, sample: e.error });
    if (errorGroups.size > 200) {
      const smallest = [...errorGroups.entries()].sort((a, b) => a[1].count - b[1].count)[0];
      errorGroups.delete(smallest[0]);
    }
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

  function currentSessionCap() {
    if (admissionCfg.enabled === false) return capacity;
    const mult = Number(admissionCfg.perGateSlot || 3);
    const floor = Math.min(capacity, Number(admissionCfg.minActive || 100));
    return Math.min(capacity, Math.max(floor, Math.ceil(gate.limit * mult)));
  }

  function fill() {
    if (stopping || paused) return;
    const sessionCap = currentSessionCap();
    const maxPerWorker = Math.ceil(capacity / shardCount);
    while (queue.length) {
      const active = workers.reduce((n, w) => n + w.tasks.size, 0);
      if (active >= sessionCap) return;
      const available = workers
        .filter(w => w.alive && w.ready && w.tasks.size < maxPerWorker)
        .sort((a, b) => a.tasks.size - b.tasks.size);
      if (!available.length) return;
      const id = queue.shift();
      const s = states[id];
      if (!s || s.status !== 'pending') continue;
      const w = available[0];
      const now = Date.now();
      s.status = 'running';
      s.attempt = (s.attempt || 0) + 1;
      s.worker = w.index;
      s.startedAt = now;
      s.lastActivityAt = now;
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

  async function buildSampleReport() {
    const completedIds = Object.values(states)
      .filter(s => s.status === 'complete')
      .map(s => s.task);
    const failedIds = Object.values(states)
      .filter(s => s.status === 'failed')
      .map(s => s.task);
    const sampleDeltaTokens = {
      inputTokens: usage.inputTokens - initialUsage.inputTokens,
      outputTokens: usage.outputTokens - initialUsage.outputTokens,
      costUsd: Number((usage.costUsd - initialUsage.costUsd).toFixed(6)),
    };
    const scale = completedIds.length > 0 ? dbTasks.length / completedIds.length : 0;
    const metricDeltas = {};
    for (const [k, v] of Object.entries(metrics)) {
      const d = v - (initialMetrics[k] || 0);
      if (d !== 0) metricDeltas[k] = d;
    }
    let customSummary = null;
    if (typeof job.sampleSummary === 'function') {
      try {
        customSummary = await job.sampleSummary({
          completedTaskIds: completedIds,
          failedTaskIds: failedIds,
          metricDeltas,
          sampleUsage: sampleDeltaTokens,
          runtimeDb,
        });
      } catch (err) {
        customSummary = { error: err.message };
      }
    }
    return {
      sampleSize: activeTasks.length,
      completed: completedIds.length,
      failed: failedIds.length,
      catalogTotalTasks: dbTasks.length,
      sampleUsage: sampleDeltaTokens,
      estimatedFullRun: {
        inputTokens: Math.round(sampleDeltaTokens.inputTokens * scale),
        outputTokens: Math.round(sampleDeltaTokens.outputTokens * scale),
        costUsd: Number((sampleDeltaTokens.costUsd * scale).toFixed(4)),
      },
      metricDeltas,
      errorGroups: [...errorGroups.entries()].map(([key, g]) => ({ key, count: g.count })),
      customSummary,
      inspectHint: completedIds[0]
        ? `pi-swarm inspect --job ${jobPath} ${completedIds[0]}`
        : null,
    };
  }

  async function checkAllTerminal() {
    const all = Object.values(states);
    if (
      all.length > 0 &&
      all.every(s => s.status === 'complete' || s.status === 'failed') &&
      !finishedAt
    ) {
      finishedAt = new Date().toISOString();
      const failedCount = all.filter(s => s.status === 'failed').length;
      const doneCount = all.filter(s => s.status === 'complete').length;
      await persistHeartbeat(true);
      await notifyWebhook(
        failedCount > 0 ? 'completed_with_failures' : 'completed',
        `Run finished: ${doneCount}/${all.length} complete, ${failedCount} failed, cost $${usage.costUsd.toFixed(4)}`
      );
      if (RUN.sampleMode) {
        const sampleReport = await buildSampleReport();
        console.log(JSON.stringify({ sampleReport }, null, 2));
      }
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
    if (message.stopReason === 'already_complete') skipped++;
    if (message.error) {
      s.failures = (s.failures || 0) + 1;
      s.lastError = redact(message.error);
      recordError(id, message.error);
      if (s.failures >= maxFailures) {
        s.status = 'failed';
        await runtimeDb.markTaskFailed(job.runId, id, s.failures, s.lastError).catch(() => {});
        await checkAllTerminal();
      } else {
        s.status = 'backoff';
        const backoffMs = Math.min(maxBackoffMs, baseBackoffMs * Math.pow(1.5, Math.min(8, s.failures - 1)));
        s.retryAt = Date.now() + backoffMs;
        await runtimeDb.markTaskBackoff(job.runId, id, s.failures, s.lastError).catch(() => {});
      }
    } else {
      s.status = 'complete';
      const keepSess = Boolean(job.retainSessions || RUN.sampleMode || message.retainSession);
      await runtimeDb.markTaskComplete(job.runId, id, { retainSession: keepSess }).catch(() => {});
      await checkAllTerminal();
    }
    fill();
  }

  function touchTask(taskId) {
    const s = taskId ? states[taskId] : null;
    if (s) s.lastActivityAt = Date.now();
  }

  function onMessage(w, m) {
    if (m.type === 'llm_acquire') {
      touchTask(m.taskId);
      gate.acquire(
        m.id,
        w.index,
        () => {
          touchTask(m.taskId);
          if (w.child.connected) w.child.send({ type: 'llm_grant', id: m.id });
        },
        { priority: m.priority || 0, taskId: m.taskId || null, estTokens: m.estTokens || 0 }
      );
      return;
    }
    if (m.type === 'llm_release') {
      gate.release(m.id, m.status);
      if (Number.isFinite(m.waitMs)) waitSamples.push(m.waitMs);
      if (m.endpoint) {
        const es = (endpointStats[m.endpoint] ??= { ok: 0, backoff: 0, other: 0 });
        if (m.status === 429 || m.status === 503 || m.status >= 500) es.backoff++;
        else if (m.status >= 200 && m.status < 400) es.ok++;
        else if (m.status > 0) es.other++;
      }
      if (breakerCfg.enabled !== false && m.status >= 400 && !paused) {
        const tripped = gate.checkCircuitBreaker(breakerCfg);
        if (tripped) {
          pauseSwarm(tripped.reason, 'circuit_breaker');
        }
      }
      // As AIMD increases gate.limit, session admission cap grows -> top up sessions
      fill();
      return;
    }
    if (m.type === 'slot_acquire') {
      if (!slotPools[m.pool]) {
        slotPools[m.pool] = new SlotPool(120, 0);
      }
      slotPools[m.pool].acquire(
        m.id,
        w.index,
        () => w.child.connected && w.child.send({ type: 'slot_grant', pool: m.pool, id: m.id })
      );
      return;
    }
    if (m.type === 'slot_release') {
      slotPools[m.pool]?.release(m.id);
      return;
    }
    if (m.type === 'usage') {
      touchTask(m.task);
      const inp = Number(m.input) || 0;
      const out = Number(m.output) || 0;
      const cr = Number(m.cacheRead) || 0;
      const cw = Number(m.cacheWrite) || 0;
      const turnCost = computeTurnCostUsd({ input: inp, output: out, cacheRead: cr, cacheWrite: cw }, job.model.cost);
      usage.inputTokens += inp;
      usage.outputTokens += out;
      usage.cacheReadTokens += cr;
      usage.cacheWriteTokens += cw;
      usage.costUsd = Number((usage.costUsd + turnCost).toFixed(8));
      gate.recordTokens(out);
      if (m.task) {
        void runtimeDb
          .addTaskUsage(job.runId, m.task, { inputTokens: inp, outputTokens: out, costUsd: turnCost })
          .catch(() => {});
      }
      if (budgetUsd !== null && budgetUsd > 0 && usage.costUsd >= budgetUsd && !paused) {
        pauseSwarm(
          `budget_exceeded: $${usage.costUsd.toFixed(4)} >= budget $${budgetUsd.toFixed(4)}`,
          'budget_exceeded'
        );
      }
      return;
    }
    if (m.type === 'metric') {
      metrics[m.name] = (metrics[m.name] || 0) + (Number(m.delta) || 0);
      return;
    }
    if (m.type === 'ready') {
      w.ready = true;
      fill();
      return;
    }
    if (m.type === 'session_ready') {
      touchTask(m.task);
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

    touchTask(m.task);
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
        ...(RUN.sampleMode ? { PI_SWARM_RETAIN_SESSIONS: '1' } : {}),
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
          // Worker process crash is not counted against task maxFailures
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

  await checkAllTerminal();
  if (stopping) return donePromise;

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
    const reply = obj => {
      conn.write(JSON.stringify(obj) + '\n');
      conn.end();
    };
    try {
      const cmd = JSON.parse(line);
      if (cmd.action === 'status') {
        return reply({ ok: true, live: buildLiveObject() });
      }
      if (cmd.action === 'pause') {
        pauseSwarm(cmd.reason || 'manual_pause', 'paused');
        return reply({ ok: true, paused: true, pauseReason });
      }
      if (cmd.action === 'resume') {
        resumeSwarm();
        return reply({ ok: true, paused: false });
      }
      if (cmd.action === 'budget') {
        budgetUsd = cmd.usd === null || cmd.usd === 'none' ? null : Number(cmd.usd);
        if (paused && String(pauseReason).startsWith('budget_exceeded') && (budgetUsd === null || usage.costUsd < budgetUsd)) {
          resumeSwarm();
        }
        await persistHeartbeat(false);
        return reply({ ok: true, budgetUsd, costUsd: usage.costUsd, paused });
      }
      if (cmd.action === 'retry') {
        const resetIds = await runtimeDb.retryFailedTasks(job.runId, cmd.tasks || null);
        for (const id of resetIds) {
          const s = states[id];
          if (s && s.status !== 'running') {
            s.status = 'pending';
            s.failures = 0;
            s.lastError = null;
            queue.push(id);
          }
        }
        finishedAt = null;
        fill();
        await persistHeartbeat(false);
        return reply({ ok: true, retriedCount: resetIds.length, tasks: resetIds });
      }
      if (cmd.action === 'inspect') {
        const info = await runtimeDb.inspectTask(job.runId, cmd.taskId);
        if (!info) return reply({ ok: false, error: `Task not found: ${cmd.taskId}` });
        return reply({ ok: true, task: info });
      }
      if (cmd.action === 'set_limit') {
        if (cmd.pool === 'gate') {
          gate.setLimits({ limit: cmd.limit, min: cmd.min, max: cmd.max, tpm: cmd.tpm });
          fill();
          return reply({ ok: true, gate: gate.snapshot(false) });
        }
        if (!slotPools[cmd.pool]) {
          slotPools[cmd.pool] = new SlotPool(cmd.limit || 120, cmd.perMinute || 0);
        } else {
          slotPools[cmd.pool].setLimits({ limit: cmd.limit, perMinute: cmd.perMinute });
        }
        return reply({ ok: true, pool: cmd.pool, ...slotPools[cmd.pool].snapshot() });
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
        return reply({
          ok: true,
          id,
          workersAcked: acks.length,
          matched: acks.reduce((n, a) => n + (a.matched || 0), 0),
          queuedSteer: acks.reduce((n, a) => n + (a.queuedSteer || 0), 0),
          interruptedAndSteered: acks.reduce((n, a) => n + (a.interruptedAndSteered || 0), 0),
          startedImmediately: acks.reduce((n, a) => n + (a.startedImmediately || 0), 0),
        });
      }
      if (cmd.action === 'stop') {
        reply({ ok: true, stopping: true });
        await shutdown(0);
        return;
      }
      return reply({ ok: false, error: `Unknown action: ${cmd.action}` });
    } catch (err) {
      return reply({ ok: false, error: err.message });
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
      out[k] = sp.snapshot();
    }
    return out;
  }

  function buildLiveObject() {
    const all = Object.values(states);
    const done = all.filter(s => s.status === 'complete').length;
    const active = all.filter(s => s.status === 'running').length;
    const backoff = all.filter(s => s.status === 'backoff').length;
    const failedPerm = all.filter(s => s.status === 'failed').length;
    return {
      run: RUN,
      at: new Date().toISOString(),
      paused,
      pauseReason,
      finishedAt,
      elapsedSec: Math.round(
        ((finishedAt ? Date.parse(finishedAt) : Date.now()) - Date.parse(RUN.startedAt)) / 1000
      ),
      processUptimeSec: Math.round((Date.now() - startedMs) / 1000),
      resumedSessions,
      sessionCap: currentSessionCap(),
      maxTaskFailures: maxFailures,
      tasks: {
        total: all.length,
        done,
        ok: done,
        failed: failedPerm,
        backoff,
        skipped,
        active,
      },
      usage: { ...usage },
      budgetUsd,
      timeoutMs: Number(job.httpIdleTimeoutMs || 0),
      gate: gate.snapshot(false),
      endpoints: { ...endpointStats },
      pools: buildPoolsSnapshot(),
      metrics: { ...metrics },
      errorGroups: [...errorGroups.entries()]
        .map(([key, g]) => ({ key, ...g }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 12),
      failedTasks: all
        .filter(s => s.status === 'failed')
        .slice(0, 25)
        .map(s => ({ id: s.task, failures: s.failures, error: s.lastError })),
      longestRunning: all
        .filter(s => s.status === 'running' && s.startedAt)
        .sort((a, b) => a.startedAt - b.startedAt)
        .slice(0, 5)
        .map(s => ({
          id: s.task,
          runningSec: Math.round((Date.now() - s.startedAt) / 1000),
          attempt: s.attempt,
        })),
      pi: { retries: piRetries, continuations: piContinuations, stalledAborts },
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
        metrics,
        usage,
        budgetUsd,
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
      job,
    });
    console.log(`pi-swarm dashboard listening on http://${dashboardHost}:${effectiveDashPort}`);
  }

  const liveTimer = setInterval(() => {
    const now = Date.now();
    if (!paused) {
      for (const s of Object.values(states)) {
        if (s.status === 'backoff' && now >= (s.retryAt || 0)) {
          s.status = 'pending';
          queue.push(s.task);
        }
      }
      fill();
    }
    void persistHeartbeat(false);
  }, 2000);

  const tickIntervalMs = Math.max(100, Number(job.tickIntervalMs || 10_000));
  const tickTimer = setInterval(() => {
    const now = Date.now();
    // Stuck-session detector: abort sessions idle past stallTimeoutMs with zero gate activity
    if (stallTimeoutMs > 0 && !paused) {
      for (const s of Object.values(states)) {
        if (
          s.status === 'running' &&
          s.lastActivityAt &&
          now - s.lastActivityAt > stallTimeoutMs &&
          !gate.hasTaskActivity(s.task)
        ) {
          const w = workers[s.worker];
          if (w?.alive && w.child.connected) {
            stalledAborts++;
            s.lastActivityAt = now;
            w.child.send({
              type: 'abort_task',
              taskId: s.task,
              reason: `stalled_session: no progress or LLM activity for ${Math.round(stallTimeoutMs / 1000)}s`,
            });
          }
        }
      }
    }

    const s = gate.snapshot(true);
    const all = Object.values(states);
    const done = all.filter(x => x.status === 'complete').length;
    timeline.push({
      at: new Date().toISOString(),
      t: Math.round((now - startedMs) / 1000),
      ...s,
      done,
      waitP50: pct(waitSamples, 0.5),
      waitP95: pct(waitSamples, 0.95),
    });
    if (timeline.length > 720) timeline.shift();
    waitSamples.length = 0;
    void persistHeartbeat(true);
  }, tickIntervalMs);

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
    const finalLive = latestSnapshot || buildLiveObject();
    await runtimeDb.close();
    resolveDone({
      completed: finalLive.tasks.done,
      failed: finalLive.tasks.failed,
      usage: finalLive.usage,
      metrics: finalLive.metrics,
      snapshot: finalLive,
    });
    if (exitProcess) {
      process.exit(code);
    }
  }

  const onSigTerm = () => void shutdown(0);
  const onSigInt = () => void shutdown(0);
  if (exitProcess) {
    process.on('SIGTERM', onSigTerm);
    process.on('SIGINT', onSigInt);
  }

  return donePromise;
}
