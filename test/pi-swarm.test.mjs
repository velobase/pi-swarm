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

  const owned = ['qwen-image-2-1', 'gpt-quiz-maker-for-google-forms'];
  assert.equal(fuzzyResolveId(owned, 'qwen_image_2_1'), 'qwen-image-2-1');
  assert.equal(fuzzyResolveId(owned, 'gpt-quiz-maker'), 'gpt-quiz-maker-for-google-forms');
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

  // Release r1 with 200 -> priority retry-4 MUST jump ahead of cold-3
  gate.release('r1', 200);
  assert.deepEqual(order, ['r1', 'r2', 'retry-4']);
  assert.equal(gate.snapshot().rawLimit, 2.5);

  // Worker 1 crashes while holding r2 -> slot freed immediately and cold-3 is granted
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

  assert.equal(granted, 2); // concurrency capped at 2
  pool.release('a');
  assert.equal(granted, 3); // 3rd granted (hits perMinute=3)
  pool.release('b');
  assert.equal(granted, 3); // 4th held back by perMinute=3 cap!
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
    { type: 'message', message: { role: 'toolResult', content: 'fetched page' } },
    { type: 'message', message: { role: 'assistant', stopReason: 'error', errorMessage: '401' } },
  ];
  const cleaned = sanitizeSessionEntriesForResume(entries);
  assert.equal(cleaned.length, 4);
  assert.equal(cleaned.at(-1).message.role, 'toolResult');
});

test('defineSwarm validates required fields and applies safe defaults', () => {
  const spec = defineSwarm({
    runId: 'unit-run',
    model: { provider: 'vertex-ds', id: 'deepseek-ai/deepseek-v4.1-flash-maas', vertexProject: 'p' },
    tasks: [{ id: '1', payload: {} }],
    buildPrompt: () => 'go',
  });
  assert.equal(spec.concurrency, 3000);
  assert.equal(spec.httpIdleTimeoutMs, 1_800_000);
  assert.equal(spec.retry.maxRetries, 30);
});

test('RuntimeDb persists tasks, sessions, fetch_cache, and learned gate limits in PostgreSQL', async t => {
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
    pools: { curl: { limit: 300 } },
    jinaCalls: 12,
    jinaTokens: 9500,
    snapshot: { tasks: { total: 2, done: 0 } },
    timeline: [{ t: 10, limit: 880 }],
  });

  // Simulate process restart: running task resets to pending, learned gateLimit (880) is inherited, session is restored clean
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
  assert.equal(init2.jinaTokens, 9500);

  const tasks = await db.loadTasks(runId);
  assert.equal(tasks[0].status, 'pending');

  const restoredSession = await db.loadSession(runId, '0001');
  assert.equal(restoredSession.length, 2);

  // Fetch cache roundtrip
  await db.putCachedFetch('https://example.com', 'curl', true, { ok: true, httpStatus: 200, title: 'Example' });
  const hit = await db.getCachedFetch('https://example.com', 'curl', 3600);
  assert.equal(hit.cached, true);
  assert.equal(hit.title, 'Example');

  // Task completion prunes session entry automatically
  await db.markTaskComplete(runId, '0001', { retainSession: false });
  const afterComplete = await db.loadSession(runId, '0001');
  assert.equal(afterComplete, null);

  await db.pool.query(`DELETE FROM swarm_runs WHERE run_id = $1`, [runId]);
  await db.close();
});
