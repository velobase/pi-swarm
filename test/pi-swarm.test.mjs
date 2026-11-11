import test from 'node:test';
import assert from 'node:assert/strict';
import {
  defineSwarm,
  parseMaybeJson,
  normalizeKeyedMap,
  fuzzyResolveId,
  Gate,
  SlotPool,
  RuntimeDb,
} from '../src/index.mjs';
import { sanitizeSessionEntriesForResume } from '../src/runtime-db.mjs';

test('liberal input normalizers handle stringified JSON and fuzzy IDs', () => {
  assert.deepEqual(parseMaybeJson('{"a":1}'), { a: 1 });
  assert.equal(parseMaybeJson('plain text'), 'plain text');

  const localeMap = normalizeKeyedMap('[{"locale":"zh-CN","name":"你好"},{"locale":"ja","name":"こんにちは"}]', {
    keyField: 'locale',
    aliases: { 'zh-CN': 'zh', 'zh-TW': 'tw' },
  });
  assert.equal(localeMap.zh.name, '你好');
  assert.equal(localeMap.ja.name, 'こんにちは');

  const owned = ['item-alpha-2-1', 'item-beta-long-slug'];
  assert.equal(fuzzyResolveId(owned, 'item_alpha_2_1'), 'item-alpha-2-1');
  assert.equal(fuzzyResolveId(owned, 'item-beta'), 'item-beta-long-slug');
  assert.equal(fuzzyResolveId(owned, 'completely-unrelated'), null);
});

test('Gate enforces AIMD, priority retry queue, and worker crash cleanup', () => {
  const gate = new Gate({
    limit: 2,
    min: 1,
    max: 10,
    increaseStep: 0.5,
    decreaseFactor: 0.5,
    cooldownMinMs: 20,
    cooldownJitterMs: 5,
  });

  const order = [];
  gate.acquire('r1', 0, () => order.push('r1'));
  gate.acquire('r2', 1, () => order.push('r2'));
  gate.acquire('cold-3', 0, () => order.push('cold-3'), { priority: 0 });
  gate.acquire('retry-4', 0, () => order.push('retry-4'), { priority: 1 });

  assert.deepEqual(order, ['r1', 'r2']);
  assert.equal(gate.inflight, 2);
  assert.equal(gate.waitingCount, 2);

  gate.release('r1', 200);
  assert.deepEqual(order, ['r1', 'r2', 'retry-4']);
  assert.equal(gate.snapshot().rawLimit, 2.5);

  gate.dropWorker(1);
  assert.deepEqual(order, ['r1', 'r2', 'retry-4', 'cold-3']);
  gate.close();
});

test('SlotPool enforces concurrency and sliding-window RPM limits', () => {
  const pool = new SlotPool(2, 3);
  let granted = 0;
  pool.acquire('a', 0, () => granted++);
  pool.acquire('b', 0, () => granted++);
  pool.acquire('c', 0, () => granted++);
  pool.acquire('d', 0, () => granted++);

  assert.equal(granted, 2);
  pool.release('a');
  assert.equal(granted, 3);
  pool.release('b');
  assert.equal(granted, 3);
  assert.equal(pool.snapshot().waiting, 1);

  pool.setLimits({ perMinute: 10 });
  assert.equal(granted, 4);
  pool.close();
});

test('sanitizeSessionEntriesForResume strips trailing errored assistant turn', () => {
  const entries = [
    { type: 'session', id: 's1' },
    { type: 'message', message: { role: 'user', content: 'hi' } },
    { type: 'message', message: { role: 'assistant', stopReason: 'toolUse', content: [] } },
    { type: 'message', message: { role: 'toolResult', content: 'tool output' } },
    { type: 'message', message: { role: 'assistant', stopReason: 'error', errorMessage: '401' } },
  ];
  const cleaned = sanitizeSessionEntriesForResume(entries);
  assert.equal(cleaned.length, 4);
  assert.equal(cleaned.at(-1).message.role, 'toolResult');
});

test('defineSwarm has zero hardcoded task-specific pools', () => {
  const spec = defineSwarm({
    runId: 'unit-run',
    model: { provider: 'v', id: 'm', baseUrl: 'https://example.com/v1' },
    tasks: [{ id: '1', payload: {} }],
    buildPrompt: () => 'go',
  });
  assert.equal(spec.concurrency, 3000);
  assert.equal(spec.httpIdleTimeoutMs, 1_800_000);
  assert.deepEqual(spec.pools, {});
});

test('RuntimeDb persists tasks, sessions, generic KV cache, metrics, and learned gate limits', async t => {
  const url = process.env.TEST_SWARM_DATABASE_URL;
  if (!url) {
    t.skip('TEST_SWARM_DATABASE_URL not set');
    return;
  }
  const db = new RuntimeDb(url, { max: 4 });
  await db.migrate();
  const runId = 'test-run-' + Date.now();

  const init1 = await db.initRun({
    runId,
    name: 'Integration Test',
    config: { concurrency: 10 },
    tasks: [
      { id: '0001', payload: { items: [1, 2] } },
      { id: '0002', payload: { items: [3, 4] } },
    ],
    defaultGateLimit: 250,
  });
  assert.equal(init1.gateLimit, 250);

  await db.markTaskRunning(runId, '0001', 0, 1);
  await db.saveSession(runId, '0001', [
    { type: 'session', id: '0001' },
    { type: 'message', message: { role: 'user', content: 'start' } },
    { type: 'message', message: { role: 'assistant', stopReason: 'error', errorMessage: 'timeout' } },
  ]);
  await db.saveHeartbeat(runId, {
    gateLimit: 880,
    pools: { githubApi: { limit: 80 } },
    metrics: { api_calls: 42, tokens_used: 9500 },
    snapshot: { tasks: { total: 2, done: 0 } },
    timeline: [{ t: 10, limit: 880 }],
  });

  const init2 = await db.initRun({
    runId,
    name: 'Integration Test',
    config: { concurrency: 10 },
    tasks: [
      { id: '0001', payload: { items: [1, 2] } },
      { id: '0002', payload: { items: [3, 4] } },
    ],
    defaultGateLimit: 250,
  });
  assert.equal(init2.gateLimit, 880);
  assert.equal(init2.metrics.tokens_used, 9500);

  const tasks = await db.loadTasks(runId);
  assert.equal(tasks[0].status, 'pending');

  const restoredSession = await db.loadSession(runId, '0001');
  assert.equal(restoredSession.length, 2);

  await db.cacheSet('my-ns', 'key-1', { ok: true, data: [1, 2, 3] });
  const hit = await db.cacheGet('my-ns', 'key-1', 3600);
  assert.deepEqual(hit, { ok: true, data: [1, 2, 3] });

  await db.markTaskComplete(runId, '0001', { retainSession: false });
  const afterComplete = await db.loadSession(runId, '0001');
  assert.equal(afterComplete, null);

  await db.pool.query(`DELETE FROM swarm_runs WHERE run_id = $1`, [runId]);
  await db.pool.query(`DELETE FROM swarm_cache WHERE namespace = 'my-ns'`);
  await db.close();
});

import { errorKey, computeRate, buildProgress, evaluateHealth } from '../src/dashboard/state.mjs';

test('dashboard state: error grouping, rate, progress and health verdicts', () => {
  const a = errorKey('save_item: [{"type":"text","text":"Handle \\"foo-1\\" is not in task 0012. Valid handles: a, b"}]');
  const b = errorKey('save_item: [{"type":"text","text":"Handle \\"bar-22\\" is not in task 0931. Valid handles: c, d"}]');
  assert.equal(a.startsWith('save_item: Handle'), true);
  assert.equal(a.split('Valid')[0], b.split('Valid')[0]);

  const now = 1_000_000_000;
  const samples = [{ t: now - 240_000, done: 100 }, { t: now - 120_000, done: 180 }, { t: now, done: 300 }];
  assert.equal(computeRate(samples, now), 50);
  assert.equal(computeRate([{ t: now, done: 1 }], now), null);

  // Fallback progress ignores tasks that were already complete at start.
  const live = { at: new Date(now).toISOString(), tasks: { total: 100, done: 40, skipped: 30, active: 60 } };
  const fallback = buildProgress({ jobProgress: null, live, samples, now });
  assert.deepEqual([fallback.done, fallback.total, fallback.source], [10, 70, 'tasks']);
  const fromJob = buildProgress({ jobProgress: { done: 300, total: 800, unit: 'items' }, live, samples, now });
  assert.deepEqual([fromJob.done, fromJob.remaining, fromJob.etaSec], [300, 500, 600]);

  const row = (w, waitP95 = 1000) => ({ window: { ok: 0, backoff: 0, other: 0, net: 0, ...w }, waitP95 });
  const verdict = tl => evaluateHealth({ live, timeline: tl, progress: fromJob, timeoutMs: 300_000, now }).code;
  assert.equal(verdict([row({ ok: 100 })]), 'healthy');
  assert.equal(verdict([row({ ok: 10, other: 90 })]), 'upstream_errors');
  assert.equal(verdict([row({ ok: 100 }, 290_000)]), 'queue_wait');
  assert.equal(verdict([row({ ok: 40, net: 60 })]), 'network_failures');
  assert.equal(verdict([row({ ok: 60, backoff: 40 })]), 'rate_limited');
  assert.equal(evaluateHealth({ live: { ...live, at: new Date(now - 60_000).toISOString() }, timeline: [], progress: fromJob, timeoutMs: 0, now }).code, 'stale');
});

test('pgJson strips NUL escapes that jsonb rejects', async () => {
  const { pgJson } = await import('../src/runtime-db.mjs');
  assert.equal(pgJson({ a: 'x\u0000y' }), '{"a":"xy"}');
});
