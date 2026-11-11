import test from 'node:test';
import assert from 'node:assert/strict';
import {
  defineSwarm,
  parseMaybeJson,
  normalizeKeyedMap,
  fuzzyResolveId,
  Gate,
  SlotPool,
  EndpointRouter,
  RuntimeDb,
} from '../src/index.mjs';
import { sanitizeSessionEntriesForResume, pgJson } from '../src/runtime-db.mjs';
import { errorKey, computeRate, buildProgress, evaluateHealth } from '../src/dashboard/state.mjs';

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

test('Gate enforces AIMD, priority retry queue, TPM cap, pause/resume, circuit breaker, and worker crash cleanup', () => {
  const gate = new Gate({
    limit: 2,
    min: 1,
    max: 10,
    tpm: 1000,
    increaseStep: 0.5,
    decreaseFactor: 0.5,
    cooldownMinMs: 20,
    cooldownJitterMs: 5,
  });

  const order = [];
  gate.acquire('r1', 0, () => order.push('r1'), { taskId: 't1', estTokens: 400 });
  gate.acquire('r2', 1, () => order.push('r2'), { taskId: 't2', estTokens: 400 });
  gate.acquire('cold-3', 0, () => order.push('cold-3'), { priority: 0, taskId: 't3', estTokens: 150 });
  gate.acquire('retry-4', 0, () => order.push('retry-4'), { priority: 1, taskId: 't4', estTokens: 150 });

  assert.deepEqual(order, ['r1', 'r2']);
  assert.equal(gate.hasTaskActivity('t1'), true);
  assert.equal(gate.hasTaskActivity('t4'), true);
  assert.equal(gate.hasTaskActivity('t99'), false);

  // Release r1 -> priority retry-4 jumps ahead (800 + 150 <= 1000 TPM)
  gate.release('r1', 200);
  assert.deepEqual(order, ['r1', 'r2', 'retry-4']);

  // Worker 1 crashes -> r2 freed, now usedTokens = 950 so cold-3 (150 tok) is held by TPM=1000!
  gate.dropWorker(1);
  assert.deepEqual(order, ['r1', 'r2', 'retry-4']);
  assert.equal(gate.waitingCount, 1);

  // Raising TPM lets cold-3 through immediately
  gate.setLimits({ tpm: 2000 });
  assert.deepEqual(order, ['r1', 'r2', 'retry-4', 'cold-3']);

  // Free one slot so capacity is available, then test pause & resume
  gate.release('retry-4', 200);
  gate.pause('manual_test');
  gate.acquire('r5', 0, () => order.push('r5'));
  assert.equal(order.includes('r5'), false);
  gate.resume();
  assert.equal(order.includes('r5'), true);

  // Circuit breaker on 401 storm
  for (let i = 0; i < 10; i++) gate.release(`f${i}`, 401);
  const tripped = gate.checkCircuitBreaker({ minRequests: 8, fatalShare: 0.5 });
  assert.equal(tripped.tripped, true);
  assert.equal(tripped.topStatus, 401);
  gate.close();
});

test('EndpointRouter round-robins primaries and switches to fallback on cooldown', () => {
  const router = new EndpointRouter([
    { name: 'p1', baseUrl: 'https://p1.example/v1', apiKey: 'k1', cooldownMs: 5000 },
    { name: 'p2', baseUrl: 'https://p2.example/v1', apiKey: 'k2', cooldownMs: 5000 },
    { name: 'fb', baseUrl: 'https://fb.example/v1', model: 'backup-model', apiKey: 'k3', fallback: true },
  ]);

  const a = router.pick();
  const b = router.pick();
  assert.equal(a.name, 'p1');
  assert.equal(b.name, 'p2');

  router.report(a, 429);
  router.report(b, 503);

  const c = router.pick();
  assert.equal(c.name, 'fb');
  assert.equal(c.model, 'backup-model');
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

test('sanitizeSessionEntriesForResume and pgJson', () => {
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
  assert.equal(pgJson({ a: 'x\u0000y' }), '{"a":"xy"}');
});

test('defineSwarm has zero hardcoded task-specific pools and exposes safety defaults', () => {
  const spec = defineSwarm({
    runId: 'unit-run',
    model: { provider: 'v', id: 'm', baseUrl: 'https://example.com/v1' },
    tasks: [{ id: '1', payload: {} }],
    buildPrompt: () => 'go',
  });
  assert.equal(spec.concurrency, 3000);
  assert.equal(spec.httpIdleTimeoutMs, 1_800_000);
  assert.equal(spec.circuitBreaker.enabled, true);
  assert.equal(spec.taskRetries.maxFailures, 5);
  assert.deepEqual(spec.pools, {});
});

test('dashboard state: error grouping, rate, progress, paused/breaker/failure verdicts', () => {
  const a = errorKey('save_item: [{"type":"text","text":"Handle \\"foo-1\\" is not in task 0012. Valid handles: a, b"}]');
  const b = errorKey('save_item: [{"type":"text","text":"Handle \\"bar-22\\" is not in task 0931. Valid handles: c, d"}]');
  assert.equal(a.split('Valid')[0], b.split('Valid')[0]);

  const now = 1_000_000_000;
  const samples = [{ t: now - 240_000, done: 100 }, { t: now - 120_000, done: 180 }, { t: now, done: 300 }];
  assert.equal(computeRate(samples, now), 50);

  const live = { at: new Date(now).toISOString(), tasks: { total: 100, done: 40, skipped: 30, failed: 0, active: 60 } };
  const fromJob = buildProgress({ jobProgress: { done: 300, total: 800, unit: 'items' }, live, samples, now });

  assert.equal(
    evaluateHealth({ live: { ...live, paused: true, pauseReason: 'circuit_breaker: 401' }, timeline: [], progress: fromJob, timeoutMs: 300_000, now }).code,
    'circuit_breaker'
  );
  assert.equal(
    evaluateHealth({ live: { ...live, paused: true, pauseReason: 'budget_exceeded: $5' }, timeline: [], progress: fromJob, timeoutMs: 300_000, now }).code,
    'budget_exceeded'
  );
  assert.equal(
    evaluateHealth({ live: { ...live, finishedAt: new Date(now).toISOString(), tasks: { ...live.tasks, failed: 3 } }, timeline: [], progress: fromJob, timeoutMs: 300_000, now }).code,
    'finished_with_failures'
  );
});

test('RuntimeDb persists tasks, failures, retry, inspect, usage, and learned gate limits', async t => {
  const url = process.env.TEST_SWARM_DATABASE_URL;
  if (!url) {
    t.skip('TEST_SWARM_DATABASE_URL not set');
    return;
  }
  const db = new RuntimeDb(url, { max: 4 });
  await db.migrate();
  const runId = 'test-run-' + Date.now();

  await db.initRun({
    runId,
    name: 'Integration Test',
    config: { concurrency: 10 },
    tasks: [
      { id: '0001', payload: { items: [1, 2] } },
      { id: '0002', payload: { items: [3, 4] } },
    ],
    defaultGateLimit: 250,
    defaultBudgetUsd: 12.5,
  });

  await db.markTaskRunning(runId, '0001', 0, 1);
  await db.addTaskUsage(runId, '0001', { inputTokens: 1200, outputTokens: 400, costUsd: 0.015 });
  await db.saveSession(runId, '0001', [
    { type: 'session', id: '0001' },
    { type: 'message', message: { role: 'user', content: 'start' } },
  ]);
  await db.markTaskFailed(runId, '0002', 5, 'fatal_item_err');

  const inspected = await db.inspectTask(runId, '0001');
  assert.equal(inspected.input_tokens, 1200);
  assert.equal(inspected.entries.length, 2);

  // Restart keeps failed task as failed, resets running task to pending
  await db.initRun({
    runId,
    name: 'Integration Test',
    config: { concurrency: 10 },
    tasks: [],
    defaultGateLimit: 250,
  });
  const afterRestart = await db.loadTasks(runId);
  assert.equal(afterRestart.find(x => x.id === '0001').status, 'pending');
  assert.equal(afterRestart.find(x => x.id === '0002').status, 'failed');

  // retryFailedTasks resets failed back to pending
  const retried = await db.retryFailedTasks(runId);
  assert.deepEqual(retried, ['0002']);
  const afterRetry = await db.loadTasks(runId);
  assert.equal(afterRetry.find(x => x.id === '0002').status, 'pending');

  await db.pool.query(`DELETE FROM swarm_runs WHERE run_id = $1`, [runId]);
  await db.close();
});
