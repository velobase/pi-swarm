/**
 * Automated End-to-End tests for @velobase/pi-swarm using a real local Fake OpenAI SSE server,
 * real forked worker.mjs processes, real Pi SDK sessions, and PostgreSQL state persistence.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import net from 'node:net';
import { runSupervisor, defaultSocketPath } from '../src/supervisor.mjs';
import { RuntimeDb } from '../src/runtime-db.mjs';

function sendControl(sockPath, payload) {
  return new Promise((resolve, reject) => {
    const c = net.createConnection(sockPath, () => c.write(JSON.stringify(payload) + '\n'));
    let out = '';
    c.on('data', d => { out += d.toString('utf8'); });
    c.on('end', () => resolve(JSON.parse(out.trim())));
    c.on('error', reject);
  });
}

function startFakeLlmServer(handler) {
  const server = http.createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    const json = body ? JSON.parse(body) : {};
    await handler(req, res, json);
  });
  return new Promise(resolve => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        server,
        baseUrl: `http://127.0.0.1:${port}/v1`,
        close: () => new Promise(r => server.close(r)),
      });
    });
  });
}

function writeSseToolCall(res, { model = 'fake-model', toolName, args, promptTokens = 1000, completionTokens = 500 }) {
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
  const id = 'chatcmpl-' + Math.random().toString(36).slice(2);
  const callId = 'call_' + Math.random().toString(36).slice(2);
  const chunks = [
    {
      id,
      object: 'chat.completion.chunk',
      model,
      choices: [
        {
          index: 0,
          delta: {
            role: 'assistant',
            content: null,
            tool_calls: [
              { index: 0, id: callId, type: 'function', function: { name: toolName, arguments: JSON.stringify(args) } },
            ],
          },
          finish_reason: null,
        },
      ],
    },
    {
      id,
      object: 'chat.completion.chunk',
      model,
      choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }],
      usage: { prompt_tokens: promptTokens, completion_tokens: completionTokens, total_tokens: promptTokens + completionTokens },
    },
  ];
  for (const c of chunks) res.write(`data: ${JSON.stringify(c)}\n\n`);
  res.end('data: [DONE]\n\n');
}

function writeSseText(res, { model = 'fake-model', text = 'DONE', promptTokens = 800, completionTokens = 100 }) {
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
  const id = 'chatcmpl-' + Math.random().toString(36).slice(2);
  const chunks = [
    {
      id,
      object: 'chat.completion.chunk',
      model,
      choices: [{ index: 0, delta: { role: 'assistant', content: text }, finish_reason: null }],
    },
    {
      id,
      object: 'chat.completion.chunk',
      model,
      choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
      usage: { prompt_tokens: promptTokens, completion_tokens: completionTokens, total_tokens: promptTokens + completionTokens },
    },
  ];
  for (const c of chunks) res.write(`data: ${JSON.stringify(c)}\n\n`);
  res.end('data: [DONE]\n\n');
}

test('E2E: sample dry-run, token/cost accounting, inspect transcript, and full-run resume', async t => {
  const dbUrl = process.env.TEST_SWARM_DATABASE_URL;
  if (!dbUrl) {
    t.skip('TEST_SWARM_DATABASE_URL not set');
    return;
  }

  const llm = await startFakeLlmServer((_req, res, body) => {
    const messages = body.messages || [];
    const hasToolResult = messages.some(m => m.role === 'tool');
    if (!hasToolResult) {
      const userText = JSON.stringify(messages);
      const itemMatch = /"item":"(item-\d+)"/.exec(userText);
      writeSseToolCall(res, {
        model: body.model,
        toolName: 'record_done',
        args: { item: itemMatch ? itemMatch[1] : 'unknown' },
        promptTokens: 1000,
        completionTokens: 500,
      });
    } else {
      writeSseText(res, { model: body.model, text: 'DONE', promptTokens: 1200, completionTokens: 100 });
    }
  });

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-swarm-e2e-'));
  const runId = 'e2e-sample-' + Date.now();
  const jobFile = path.join(tmpDir, 'job.mjs');
  const indexUrl = new URL('../src/index.mjs', import.meta.url).href;

  fs.writeFileSync(
    jobFile,
    `import { defineSwarm } from ${JSON.stringify(indexUrl)};
export default defineSwarm({
  runId: ${JSON.stringify(runId)},
  name: 'E2E Sample & Full Run',
  concurrency: 4,
  workers: 2,
  model: {
    provider: 'fake',
    id: 'fake-v1',
    baseUrl: ${JSON.stringify(llm.baseUrl)},
    apiKey: 'test-key',
    cost: { input: 10, output: 20, cacheRead: 0, cacheWrite: 0 }, // $10/1M in, $20/1M out
  },
  tasks: Array.from({ length: 6 }, (_, i) => ({ id: 't' + (i + 1), payload: { item: 'item-' + (i + 1) } })),
  async loadPending(task, { cache }) {
    const done = await cache.get(${JSON.stringify(runId)}, task.payload.item);
    return done ? [] : [task.payload];
  },
  systemPrompt: 'Record the item and reply DONE.',
  buildPrompt: (_t, pending) => JSON.stringify(pending),
  tools: (_task, { Type, cache, metric }) => [{
    name: 'record_done',
    label: 'Record done',
    description: 'Mark item done',
    parameters: Type.Object({ item: Type.String() }),
    execute: async (_id, { item }) => {
      await cache.set(${JSON.stringify(runId)}, item, true);
      metric('recorded', 1);
      return { content: [{ type: 'text', text: 'ok:' + item }] };
    },
  }],
});`
  );

  try {
    // 1. Sample 2 tasks out of 6
    const sampleRes = await runSupervisor({
      jobPath: jobFile,
      runtimeDatabaseUrl: dbUrl,
      sampleCount: 2,
      exitProcess: false,
    });
    assert.equal(sampleRes.completed, 2);
    assert.equal(sampleRes.failed, 0);
    assert.equal(sampleRes.metrics.recorded, 2);
    assert.equal(sampleRes.usage.inputTokens, 2 * 2200);
    assert.equal(sampleRes.usage.outputTokens, 2 * 600);
    assert.ok(sampleRes.usage.costUsd > 0.05);

    // Sample mode keeps session transcript in PG for inspection
    const db = new RuntimeDb(dbUrl, { max: 2 });
    const inspected = await db.inspectTask(runId, 't1');
    assert.equal(inspected.status, 'complete');
    assert.ok(inspected.entries.length >= 3);

    // 2. Full run picks up remaining 4 tasks and skips the 2 already completed
    const fullRes = await runSupervisor({
      jobPath: jobFile,
      runtimeDatabaseUrl: dbUrl,
      exitProcess: false,
    });
    assert.equal(fullRes.completed, 6);
    assert.equal(fullRes.metrics.recorded, 6);

    await db.pool.query(`DELETE FROM swarm_runs WHERE run_id = $1`, [runId]);
    await db.pool.query(`DELETE FROM swarm_cache WHERE namespace = $1`, [runId]);
    await db.close();
  } finally {
    await llm.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('E2E: circuit breaker auto-pauses on 401 storm, fires webhook, and resumes cleanly after fix', async t => {
  const dbUrl = process.env.TEST_SWARM_DATABASE_URL;
  if (!dbUrl) {
    t.skip('TEST_SWARM_DATABASE_URL not set');
    return;
  }

  let failWith401 = true;
  const llm = await startFakeLlmServer((_req, res) => {
    if (failWith401) {
      res.writeHead(401, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'Invalid token' } }));
      return;
    }
    writeSseText(res, { text: 'DONE' });
  });

  const webhooks = [];
  const whServer = http.createServer(async (req, res) => {
    let b = '';
    for await (const c of req) b += c;
    webhooks.push(JSON.parse(b));
    res.writeHead(200).end('ok');
  });
  await new Promise(r => whServer.listen(0, '127.0.0.1', r));
  const whUrl = `http://127.0.0.1:${whServer.address().port}/hook`;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-swarm-cb-'));
  const runId = 'e2e-cb-' + Date.now();
  const jobFile = path.join(tmpDir, 'job.mjs');
  const indexUrl = new URL('../src/index.mjs', import.meta.url).href;

  fs.writeFileSync(
    jobFile,
    `import { defineSwarm } from ${JSON.stringify(indexUrl)};
export default defineSwarm({
  runId: ${JSON.stringify(runId)},
  concurrency: 6,
  workers: 2,
  maxContinuations: 0,
  retry: { enabled: false, maxRetries: 0, baseDelayMs: 50, maxAgentDelayMs: 50 },
  taskRetries: { maxFailures: 10, baseDelayMs: 100, maxDelayMs: 200 },
  circuitBreaker: { enabled: true, minRequests: 4, fatalShare: 0.5 },
  model: { provider: 'fake', id: 'fake-v1', baseUrl: ${JSON.stringify(llm.baseUrl)}, apiKey: 'k' },
  tasks: Array.from({ length: 5 }, (_, i) => ({ id: 'c' + (i + 1), payload: {} })),
  systemPrompt: 'Reply DONE.',
  buildPrompt: () => 'go',
  tools: () => [],
});`
  );

  try {
    const sock = defaultSocketPath(runId);
    const supPromise = runSupervisor({
      jobPath: jobFile,
      runtimeDatabaseUrl: dbUrl,
      webhookUrl: whUrl,
      exitProcess: false,
    });

    // Wait until circuit breaker trips and pauses the run
    let status;
    for (let i = 0; i < 40; i++) {
      await new Promise(r => setTimeout(r, 150));
      if (fs.existsSync(sock)) {
        status = await sendControl(sock, { action: 'status' }).catch(() => null);
        if (status?.live?.paused) break;
      }
    }
    assert.equal(status?.live?.paused, true);
    assert.match(status.live.pauseReason, /^circuit_breaker/);
    assert.ok(webhooks.some(w => w.event === 'circuit_breaker'));

    // Fix upstream auth and send resume command
    failWith401 = false;
    const resCmd = await sendControl(sock, { action: 'resume' });
    assert.equal(resCmd.paused, false);

    const finalRes = await supPromise;
    assert.equal(finalRes.completed, 5);
    assert.equal(finalRes.failed, 0);

    const db = new RuntimeDb(dbUrl, { max: 2 });
    await db.pool.query(`DELETE FROM swarm_runs WHERE run_id = $1`, [runId]);
    await db.close();
  } finally {
    await llm.close();
    await new Promise(r => whServer.close(r));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('E2E: poison task exits as failed at maxFailures without blocking run, then succeeds on retry', async t => {
  const dbUrl = process.env.TEST_SWARM_DATABASE_URL;
  if (!dbUrl) {
    t.skip('TEST_SWARM_DATABASE_URL not set');
    return;
  }

  let failPoison = true;
  const llm = await startFakeLlmServer((_req, res, body) => {
    const text = JSON.stringify(body.messages || []);
    if (failPoison && text.includes('POISON_TASK')) {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'Upstream exploded on poison task' } }));
      return;
    }
    writeSseText(res, { text: 'DONE' });
  });

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-swarm-fail-'));
  const runId = 'e2e-fail-' + Date.now();
  const jobFile = path.join(tmpDir, 'job.mjs');
  const indexUrl = new URL('../src/index.mjs', import.meta.url).href;

  fs.writeFileSync(
    jobFile,
    `import { defineSwarm } from ${JSON.stringify(indexUrl)};
export default defineSwarm({
  runId: ${JSON.stringify(runId)},
  concurrency: 3,
  workers: 1,
  maxContinuations: 0,
  retry: { enabled: false, maxRetries: 0, baseDelayMs: 50, maxAgentDelayMs: 50 },
  taskRetries: { maxFailures: 2, baseDelayMs: 50, maxDelayMs: 100 },
  model: { provider: 'fake', id: 'fake-v1', baseUrl: ${JSON.stringify(llm.baseUrl)}, apiKey: 'k' },
  tasks: [
    { id: 'ok-1', payload: { label: 'GOOD_1' } },
    { id: 'bad-1', payload: { label: 'POISON_TASK' } },
    { id: 'ok-2', payload: { label: 'GOOD_2' } },
  ],
  systemPrompt: 'Reply DONE.',
  buildPrompt: (_t, p) => JSON.stringify(p),
  tools: () => [],
});`
  );

  try {
    const res1 = await runSupervisor({
      jobPath: jobFile,
      runtimeDatabaseUrl: dbUrl,
      exitProcess: false,
    });
    assert.equal(res1.completed, 2);
    assert.equal(res1.failed, 1);

    const db = new RuntimeDb(dbUrl, { max: 2 });
    const badInfo = await db.inspectTask(runId, 'bad-1');
    assert.equal(badInfo.status, 'failed');
    assert.equal(badInfo.failures, 2);

    // Fix poison condition, reset failed tasks, and re-run
    failPoison = false;
    const resetIds = await db.retryFailedTasks(runId);
    assert.deepEqual(resetIds, ['bad-1']);

    const res2 = await runSupervisor({
      jobPath: jobFile,
      runtimeDatabaseUrl: dbUrl,
      exitProcess: false,
    });
    assert.equal(res2.completed, 3);
    assert.equal(res2.failed, 0);

    await db.pool.query(`DELETE FROM swarm_runs WHERE run_id = $1`, [runId]);
    await db.close();
  } finally {
    await llm.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('E2E: multi-endpoint cooldown automatically fails over to fallback model endpoint', async t => {
  const dbUrl = process.env.TEST_SWARM_DATABASE_URL;
  if (!dbUrl) {
    t.skip('TEST_SWARM_DATABASE_URL not set');
    return;
  }

  let fallbackModelsSeen = [];
  const primaryLlm = await startFakeLlmServer((_req, res) => {
    res.writeHead(429, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'Primary rate limited' } }));
  });
  const fallbackLlm = await startFakeLlmServer((_req, res, body) => {
    fallbackModelsSeen.push(body.model);
    writeSseText(res, { model: body.model, text: 'DONE' });
  });

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-swarm-ep-'));
  const runId = 'e2e-ep-' + Date.now();
  const jobFile = path.join(tmpDir, 'job.mjs');
  const indexUrl = new URL('../src/index.mjs', import.meta.url).href;

  fs.writeFileSync(
    jobFile,
    `import { defineSwarm } from ${JSON.stringify(indexUrl)};
export default defineSwarm({
  runId: ${JSON.stringify(runId)},
  concurrency: 2,
  workers: 1,
  retry: { enabled: true, maxRetries: 4, baseDelayMs: 50, maxAgentDelayMs: 100 },
  gate: { limit: 10, min: 2, max: 20, cooldownMinMs: 50, cooldownJitterMs: 10 },
  model: {
    provider: 'fake',
    id: 'primary-model',
    baseUrl: ${JSON.stringify(primaryLlm.baseUrl)},
    apiKey: 'k1',
    endpoints: [
      { name: 'primary', baseUrl: ${JSON.stringify(primaryLlm.baseUrl)}, apiKey: 'k1', cooldownMs: 60000 },
      { name: 'backup', baseUrl: ${JSON.stringify(fallbackLlm.baseUrl)}, model: 'fallback-model-v2', apiKey: 'k2', fallback: true },
    ],
  },
  tasks: [{ id: 'ep-t1', payload: {} }, { id: 'ep-t2', payload: {} }],
  systemPrompt: 'Reply DONE.',
  buildPrompt: () => 'go',
  tools: () => [],
});`
  );

  try {
    const res = await runSupervisor({
      jobPath: jobFile,
      runtimeDatabaseUrl: dbUrl,
      exitProcess: false,
    });
    assert.equal(res.completed, 2);
    assert.ok(fallbackModelsSeen.includes('fallback-model-v2'));
    assert.ok(res.snapshot.endpoints.backup.ok >= 2);

    const db = new RuntimeDb(dbUrl, { max: 2 });
    await db.pool.query(`DELETE FROM swarm_runs WHERE run_id = $1`, [runId]);
    await db.close();
  } finally {
    await primaryLlm.close();
    await fallbackLlm.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('E2E: budget cap auto-pauses when costUsd reaches budgetUsd, and online budget raise resumes', async t => {
  const dbUrl = process.env.TEST_SWARM_DATABASE_URL;
  if (!dbUrl) {
    t.skip('TEST_SWARM_DATABASE_URL not set');
    return;
  }

  const llm = await startFakeLlmServer((_req, res) => {
    writeSseText(res, { text: 'DONE', promptTokens: 100_000, completionTokens: 50_000 });
  });

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-swarm-bgt-'));
  const runId = 'e2e-budget-' + Date.now();
  const jobFile = path.join(tmpDir, 'job.mjs');
  const indexUrl = new URL('../src/index.mjs', import.meta.url).href;

  // Each task costs: (100k * $10 + 50k * $20) / 1M = $2.00. Budget is $1.50 -> pauses after 1st task!
  fs.writeFileSync(
    jobFile,
    `import { defineSwarm } from ${JSON.stringify(indexUrl)};
export default defineSwarm({
  runId: ${JSON.stringify(runId)},
  concurrency: 1,
  workers: 1,
  budgetUsd: 1.5,
  model: {
    provider: 'fake',
    id: 'fake-v1',
    baseUrl: ${JSON.stringify(llm.baseUrl)},
    apiKey: 'k',
    cost: { input: 10, output: 20, cacheRead: 0, cacheWrite: 0 },
  },
  tasks: [{ id: 'b1', payload: {} }, { id: 'b2', payload: {} }, { id: 'b3', payload: {} }],
  systemPrompt: 'Reply DONE.',
  buildPrompt: () => 'go',
  tools: () => [],
});`
  );

  try {
    const sock = defaultSocketPath(runId);
    const supPromise = runSupervisor({
      jobPath: jobFile,
      runtimeDatabaseUrl: dbUrl,
      exitProcess: false,
    });

    let status;
    for (let i = 0; i < 40; i++) {
      await new Promise(r => setTimeout(r, 120));
      if (fs.existsSync(sock)) {
        status = await sendControl(sock, { action: 'status' }).catch(() => null);
        if (status?.live?.paused) break;
      }
    }
    assert.equal(status?.live?.paused, true);
    assert.match(status.live.pauseReason, /^budget_exceeded/);
    assert.equal(status.live.tasks.done, 1);

    // Raise budget online -> auto-resumes and completes remaining tasks
    const bRes = await sendControl(sock, { action: 'budget', usd: 10 });
    assert.equal(bRes.paused, false);

    const finalRes = await supPromise;
    assert.equal(finalRes.completed, 3);
    assert.equal(finalRes.usage.costUsd, 6);

    const db = new RuntimeDb(dbUrl, { max: 2 });
    await db.pool.query(`DELETE FROM swarm_runs WHERE run_id = $1`, [runId]);
    await db.close();
  } finally {
    await llm.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('E2E: stuck-session detector aborts hung tool call and recovers task on retry', async t => {
  const dbUrl = process.env.TEST_SWARM_DATABASE_URL;
  if (!dbUrl) {
    t.skip('TEST_SWARM_DATABASE_URL not set');
    return;
  }

  let calls = 0;
  const llm = await startFakeLlmServer((_req, res, body) => {
    const hasTool = (body.messages || []).some(m => m.role === 'tool');
    if (!hasTool && calls++ === 0) {
      writeSseToolCall(res, { toolName: 'maybe_hang', args: { hang: true } });
    } else if (!hasTool) {
      writeSseToolCall(res, { toolName: 'maybe_hang', args: { hang: false } });
    } else {
      writeSseText(res, { text: 'DONE' });
    }
  });

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-swarm-stall-'));
  const runId = 'e2e-stall-' + Date.now();
  const jobFile = path.join(tmpDir, 'job.mjs');
  const indexUrl = new URL('../src/index.mjs', import.meta.url).href;

  fs.writeFileSync(
    jobFile,
    `import { defineSwarm } from ${JSON.stringify(indexUrl)};
export default defineSwarm({
  runId: ${JSON.stringify(runId)},
  concurrency: 1,
  workers: 1,
  stallTimeoutMs: 250,
  tickIntervalMs: 100,
  taskRetries: { maxFailures: 4, baseDelayMs: 50, maxDelayMs: 100 },
  model: { provider: 'fake', id: 'fake-v1', baseUrl: ${JSON.stringify(llm.baseUrl)}, apiKey: 'k' },
  tasks: [{ id: 'hang-1', payload: {} }],
  systemPrompt: 'Call maybe_hang then reply DONE.',
  buildPrompt: () => 'go',
  tools: (_task, { Type }) => [{
    name: 'maybe_hang',
    label: 'Maybe hang',
    description: 'Hangs if hang=true until aborted',
    parameters: Type.Object({ hang: Type.Boolean() }),
    execute: (_id, { hang }, signal) => {
      if (!hang) return Promise.resolve({ content: [{ type: 'text', text: 'ok' }] });
      return new Promise((_resolve, reject) => {
        signal?.addEventListener('abort', () => reject(new Error('aborted_by_stall_detector')), { once: true });
      });
    },
  }],
});`
  );

  try {
    const res = await runSupervisor({
      jobPath: jobFile,
      runtimeDatabaseUrl: dbUrl,
      exitProcess: false,
    });
    assert.equal(res.completed, 1);
    assert.equal(res.failed, 0);
    assert.ok(res.snapshot.pi.stalledAborts >= 1);

    const db = new RuntimeDb(dbUrl, { max: 2 });
    await db.pool.query(`DELETE FROM swarm_runs WHERE run_id = $1`, [runId]);
    await db.close();
  } finally {
    await llm.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
