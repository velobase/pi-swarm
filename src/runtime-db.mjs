/**
 * Task-agnostic PostgreSQL state & checkpoint store (`swarm_runtime`),
 * strictly isolated from business databases.
 */
import pg from 'pg';

const { Pool } = pg;

export const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS swarm_runs (
  run_id      text PRIMARY KEY,
  name        text NOT NULL,
  config      jsonb NOT NULL DEFAULT '{}'::jsonb,
  gate_limit  double precision NOT NULL DEFAULT 300,
  pools       jsonb NOT NULL DEFAULT '{}'::jsonb,
  metrics     jsonb NOT NULL DEFAULT '{}'::jsonb,
  usage       jsonb NOT NULL DEFAULT '{}'::jsonb,
  budget_usd  double precision,
  snapshot    jsonb NOT NULL DEFAULT '{}'::jsonb,
  timeline    jsonb NOT NULL DEFAULT '[]'::jsonb,
  started_at  timestamptz NOT NULL DEFAULT now(),
  heartbeat   timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

ALTER TABLE swarm_runs ADD COLUMN IF NOT EXISTS usage jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE swarm_runs ADD COLUMN IF NOT EXISTS budget_usd double precision;

CREATE TABLE IF NOT EXISTS swarm_tasks (
  run_id        text NOT NULL REFERENCES swarm_runs(run_id) ON DELETE CASCADE,
  task_id       text NOT NULL,
  seq           int NOT NULL DEFAULT 0,
  payload       jsonb NOT NULL DEFAULT '{}'::jsonb,
  status        text NOT NULL DEFAULT 'pending',
  worker_id     int,
  attempt       int NOT NULL DEFAULT 0,
  failures      int NOT NULL DEFAULT 0,
  input_tokens  bigint NOT NULL DEFAULT 0,
  output_tokens bigint NOT NULL DEFAULT 0,
  cost_usd      double precision NOT NULL DEFAULT 0,
  last_error    text,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, task_id)
);

ALTER TABLE swarm_tasks ADD COLUMN IF NOT EXISTS failures int NOT NULL DEFAULT 0;
ALTER TABLE swarm_tasks ADD COLUMN IF NOT EXISTS input_tokens bigint NOT NULL DEFAULT 0;
ALTER TABLE swarm_tasks ADD COLUMN IF NOT EXISTS output_tokens bigint NOT NULL DEFAULT 0;
ALTER TABLE swarm_tasks ADD COLUMN IF NOT EXISTS cost_usd double precision NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_swarm_tasks_run_status ON swarm_tasks (run_id, status, seq);

CREATE TABLE IF NOT EXISTS swarm_sessions (
  run_id      text NOT NULL,
  task_id     text NOT NULL,
  entries     jsonb NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, task_id),
  FOREIGN KEY (run_id, task_id) REFERENCES swarm_tasks(run_id, task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS swarm_cache (
  namespace   text NOT NULL,
  cache_key   text NOT NULL,
  value       jsonb NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace, cache_key)
);

CREATE TABLE IF NOT EXISTS swarm_broadcasts (
  run_id        text NOT NULL REFERENCES swarm_runs(run_id) ON DELETE CASCADE,
  broadcast_id  text NOT NULL,
  body          text NOT NULL,
  interrupt     boolean NOT NULL DEFAULT false,
  target_tasks  text[],
  consumed_by   text[] NOT NULL DEFAULT '{}',
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, broadcast_id)
);
`;

/** JSON text safe for jsonb: PostgreSQL rejects the \u0000 escape. */
export const pgJson = value => JSON.stringify(value).replace(/\\u0000/g, '');

export function sanitizeSessionEntriesForResume(entries) {
  if (!Array.isArray(entries) || entries.length === 0) return null;
  const cloned = entries.slice();
  while (cloned.length > 1) {
    const tail = cloned[cloned.length - 1];
    const msg = tail?.message || tail;
    if (msg?.role === 'assistant' && (msg?.stopReason === 'error' || msg?.stopReason === 'aborted')) {
      cloned.pop();
    } else {
      break;
    }
  }
  return cloned.length > 0 ? cloned : null;
}

export class RuntimeDb {
  constructor(connectionString, { max = 20, applicationName = 'pi-swarm-runtime' } = {}) {
    if (!connectionString) {
      throw new Error(
        'Missing runtimeDatabaseUrl / SWARM_DATABASE_URL (must point to an isolated swarm_runtime PostgreSQL database)'
      );
    }
    this.connectionString = connectionString;
    this.pool = new Pool({
      connectionString,
      max,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 15_000,
      statement_timeout: 60_000,
      application_name: applicationName,
    });
    this.pool.on('error', err => {
      console.error(JSON.stringify({ at: new Date().toISOString(), component: 'swarm-runtime-pg', error: err.message }));
    });
  }

  async migrate() {
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN');
      await c.query('SELECT pg_advisory_xact_lock(781920451)');
      await c.query(SCHEMA_SQL);
      await c.query('COMMIT');
    } catch (err) {
      await c.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      c.release();
    }
  }

  async initRun({
    runId,
    name,
    config,
    tasks,
    defaultGateLimit = 300,
    defaultPools = {},
    defaultBudgetUsd = null,
  }) {
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN');
      const runRes = await c.query(
        `INSERT INTO swarm_runs (run_id, name, config, gate_limit, pools, budget_usd)
         VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6)
         ON CONFLICT (run_id) DO UPDATE SET
           name = EXCLUDED.name,
           budget_usd = COALESCE($6, swarm_runs.budget_usd),
           heartbeat = now(),
           finished_at = NULL
         RETURNING gate_limit, pools, metrics, usage, budget_usd, started_at, timeline`,
        [
          runId,
          name || runId,
          pgJson(config || {}),
          defaultGateLimit,
          pgJson(defaultPools),
          defaultBudgetUsd,
        ]
      );

      // Reset interrupted running/backoff tasks to pending; keep 'complete' and 'failed' untouched!
      await c.query(
        `UPDATE swarm_tasks
         SET status = 'pending', worker_id = NULL, updated_at = now()
         WHERE run_id = $1 AND status IN ('running', 'backoff')`,
        [runId]
      );

      if (Array.isArray(tasks) && tasks.length > 0) {
        const chunkSize = 1000;
        for (let i = 0; i < tasks.length; i += chunkSize) {
          const batch = tasks.slice(i, i + chunkSize);
          const ids = batch.map(t => String(t.id));
          const seqs = batch.map((_, idx) => i + idx);
          const payloads = batch.map(t => pgJson(t.payload ?? t));
          await c.query(
            `INSERT INTO swarm_tasks (run_id, task_id, seq, payload, status)
             SELECT $1, u.task_id, u.seq, u.payload::jsonb, 'pending'
             FROM unnest($2::text[], $3::int[], $4::text[]) AS u(task_id, seq, payload)
             ON CONFLICT (run_id, task_id) DO NOTHING`,
            [runId, ids, seqs, payloads]
          );
        }
      }

      await c.query('COMMIT');
      const row = runRes.rows[0];
      return {
        gateLimit: Number(row.gate_limit || defaultGateLimit),
        pools: row.pools || defaultPools,
        metrics: row.metrics || {},
        usage: row.usage || {},
        budgetUsd: row.budget_usd !== null && row.budget_usd !== undefined ? Number(row.budget_usd) : null,
        startedAt: row.started_at,
        timeline: Array.isArray(row.timeline) ? row.timeline : [],
      };
    } catch (err) {
      await c.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      c.release();
    }
  }

  async loadTasks(runId) {
    const res = await this.pool.query(
      `SELECT task_id AS id, payload, status, attempt, failures, input_tokens, output_tokens, cost_usd, last_error
       FROM swarm_tasks
       WHERE run_id = $1
       ORDER BY seq ASC, task_id ASC`,
      [runId]
    );
    return res.rows;
  }

  async markTaskRunning(runId, taskId, workerId, attempt) {
    await this.pool.query(
      `UPDATE swarm_tasks
       SET status = 'running', worker_id = $3, attempt = $4, updated_at = now()
       WHERE run_id = $1 AND task_id = $2`,
      [runId, taskId, workerId, attempt]
    );
  }

  async markTaskBackoff(runId, taskId, failures, lastError) {
    await this.pool.query(
      `UPDATE swarm_tasks
       SET status = 'backoff', worker_id = NULL, failures = $3, last_error = $4, updated_at = now()
       WHERE run_id = $1 AND task_id = $2`,
      [runId, taskId, failures, String(lastError || '').slice(0, 1000)]
    );
  }

  async markTaskFailed(runId, taskId, failures, lastError) {
    await this.pool.query(
      `UPDATE swarm_tasks
       SET status = 'failed', worker_id = NULL, failures = $3, last_error = $4, updated_at = now()
       WHERE run_id = $1 AND task_id = $2`,
      [runId, taskId, failures, String(lastError || '').slice(0, 1000)]
    );
  }

  async retryFailedTasks(runId, taskIds = null) {
    const filter = Array.isArray(taskIds) && taskIds.length > 0 ? taskIds.map(String) : null;
    const res = filter
      ? await this.pool.query(
          `UPDATE swarm_tasks
           SET status = 'pending', failures = 0, last_error = NULL, updated_at = now()
           WHERE run_id = $1 AND task_id = ANY($2::text[]) AND status IN ('failed', 'backoff')
           RETURNING task_id`,
          [runId, filter]
        )
      : await this.pool.query(
          `UPDATE swarm_tasks
           SET status = 'pending', failures = 0, last_error = NULL, updated_at = now()
           WHERE run_id = $1 AND status = 'failed'
           RETURNING task_id`,
          [runId]
        );
    return res.rows.map(r => r.task_id);
  }

  async addTaskUsage(runId, taskId, { inputTokens = 0, outputTokens = 0, costUsd = 0 }) {
    await this.pool.query(
      `UPDATE swarm_tasks
       SET input_tokens = input_tokens + $3,
           output_tokens = output_tokens + $4,
           cost_usd = cost_usd + $5,
           updated_at = now()
       WHERE run_id = $1 AND task_id = $2`,
      [runId, taskId, Math.round(inputTokens), Math.round(outputTokens), Number(costUsd) || 0]
    );
  }

  async markTaskComplete(runId, taskId, { retainSession = false } = {}) {
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN');
      await c.query(
        `UPDATE swarm_tasks
         SET status = 'complete', worker_id = NULL, last_error = NULL, updated_at = now()
         WHERE run_id = $1 AND task_id = $2`,
        [runId, taskId]
      );
      if (!retainSession) {
        await c.query(`DELETE FROM swarm_sessions WHERE run_id = $1 AND task_id = $2`, [runId, taskId]);
      }
      await c.query('COMMIT');
    } catch (err) {
      await c.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      c.release();
    }
  }

  async inspectTask(runId, taskId) {
    const [taskRes, sessRes] = await Promise.all([
      this.pool.query(
        `SELECT task_id AS id, status, attempt, failures, worker_id, input_tokens, output_tokens, cost_usd, last_error, payload, updated_at
         FROM swarm_tasks
         WHERE run_id = $1 AND task_id = $2`,
        [runId, String(taskId)]
      ),
      this.pool.query(
        `SELECT entries, updated_at FROM swarm_sessions WHERE run_id = $1 AND task_id = $2`,
        [runId, String(taskId)]
      ),
    ]);
    if (!taskRes.rowCount) return null;
    const t = taskRes.rows[0];
    return {
      ...t,
      input_tokens: Number(t.input_tokens || 0),
      output_tokens: Number(t.output_tokens || 0),
      cost_usd: Number(t.cost_usd || 0),
      sessionUpdatedAt: sessRes.rows[0]?.updated_at || null,
      entries: sessRes.rows[0]?.entries || [],
    };
  }

  async listTasksByStatus(runId, status, limit = 50) {
    const res = await this.pool.query(
      `SELECT task_id AS id, status, attempt, failures, worker_id, input_tokens, output_tokens, cost_usd, last_error, updated_at
       FROM swarm_tasks
       WHERE run_id = $1 AND ($2::text IS NULL OR status = $2)
       ORDER BY updated_at DESC
       LIMIT $3`,
      [runId, status || null, Math.min(200, Math.max(1, Number(limit) || 50))]
    );
    return res.rows.map(r => ({
      ...r,
      input_tokens: Number(r.input_tokens || 0),
      output_tokens: Number(r.output_tokens || 0),
      cost_usd: Number(r.cost_usd || 0),
    }));
  }

  async loadSession(runId, taskId) {
    const res = await this.pool.query(
      `SELECT entries FROM swarm_sessions WHERE run_id = $1 AND task_id = $2`,
      [runId, taskId]
    );
    if (!res.rowCount) return null;
    return sanitizeSessionEntriesForResume(res.rows[0].entries);
  }

  async saveSession(runId, taskId, entries) {
    if (!Array.isArray(entries) || entries.length === 0) return;
    await this.pool.query(
      `INSERT INTO swarm_sessions (run_id, task_id, entries, updated_at)
       VALUES ($1, $2, $3::jsonb, now())
       ON CONFLICT (run_id, task_id) DO UPDATE SET
         entries = EXCLUDED.entries,
         updated_at = now()`,
      [runId, taskId, pgJson(entries)]
    );
  }

  async cacheGet(namespace, key, ttlSec = 0) {
    const res =
      ttlSec > 0
        ? await this.pool.query(
            `SELECT value FROM swarm_cache
             WHERE namespace = $1 AND cache_key = $2
               AND created_at >= now() - ($3::int * interval '1 second')`,
            [String(namespace), String(key), ttlSec]
          )
        : await this.pool.query(
            `SELECT value FROM swarm_cache WHERE namespace = $1 AND cache_key = $2`,
            [String(namespace), String(key)]
          );
    return res.rowCount ? res.rows[0].value : undefined;
  }

  async cacheSet(namespace, key, value) {
    await this.pool.query(
      `INSERT INTO swarm_cache (namespace, cache_key, value, created_at)
       VALUES ($1, $2, $3::jsonb, now())
       ON CONFLICT (namespace, cache_key) DO UPDATE SET
         value = EXCLUDED.value,
         created_at = now()`,
      [String(namespace), String(key), pgJson(value)]
    );
  }

  async saveBroadcast(runId, { id, text, interrupt = false, tasks = null }) {
    await this.pool.query(
      `INSERT INTO swarm_broadcasts (run_id, broadcast_id, body, interrupt, target_tasks)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (run_id, broadcast_id) DO NOTHING`,
      [runId, id, text, Boolean(interrupt), tasks]
    );
  }

  async recordBroadcastConsumed(runId, broadcastId, taskId) {
    await this.pool.query(
      `UPDATE swarm_broadcasts
       SET consumed_by = CASE WHEN $3 = ANY(consumed_by) THEN consumed_by ELSE array_append(consumed_by, $3) END
       WHERE run_id = $1 AND broadcast_id = $2`,
      [runId, broadcastId, taskId]
    );
  }

  async saveHeartbeat(
    runId,
    { gateLimit, pools, metrics, usage, budgetUsd = null, snapshot, timeline, finishedAt = null }
  ) {
    await this.pool.query(
      `UPDATE swarm_runs
       SET gate_limit = $2,
           pools = $3::jsonb,
           metrics = $4::jsonb,
           usage = $5::jsonb,
           budget_usd = $6,
           snapshot = $7::jsonb,
           timeline = COALESCE($8::jsonb, timeline),
           heartbeat = now(),
           finished_at = COALESCE($9::timestamptz, finished_at)
       WHERE run_id = $1`,
      [
        runId,
        gateLimit,
        pgJson(pools || {}),
        pgJson(metrics || {}),
        pgJson(usage || {}),
        budgetUsd,
        pgJson(snapshot || {}),
        timeline ? pgJson(timeline.slice(-720)) : null,
        finishedAt,
      ]
    );
  }

  async getRunDashboardState(runId) {
    const res = await this.pool.query(
      `SELECT run_id, name, gate_limit, pools, metrics, usage, budget_usd, snapshot, timeline, started_at, heartbeat, finished_at
       FROM swarm_runs
       WHERE run_id = $1`,
      [runId]
    );
    if (!res.rowCount) return null;
    const r = res.rows[0];
    return {
      runId: r.run_id,
      name: r.name,
      snapshot: r.snapshot,
      timeline: Array.isArray(r.timeline) ? r.timeline : [],
      heartbeat: r.heartbeat,
      finishedAt: r.finished_at,
    };
  }

  async close() {
    await this.pool.end().catch(() => {});
  }
}
