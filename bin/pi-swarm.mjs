#!/usr/bin/env node
/**
 * CLI for @velobase/pi-swarm:
 *   pi-swarm run <job.mjs> [--concurrency 3000] [--workers 30] [--sample 30] [--budget 25] [--webhook <url>] [--dashboard 3010] [--runtime-db <url>]
 *   pi-swarm inspect (--run <runId> | --job <job.mjs>) <taskId> [--json] [--runtime-db <url>]
 *   pi-swarm retry   (--run <runId> | --job <job.mjs>) [--tasks t1,t2] [--runtime-db <url>]
 *   pi-swarm pause   (--run <runId> | --job <job.mjs>) [reason]
 *   pi-swarm resume  (--run <runId> | --job <job.mjs>)
 *   pi-swarm budget  (--run <runId> | --job <job.mjs>) --usd <amount|none>
 *   pi-swarm steer   (--run <runId> | --job <job.mjs>) [--interrupt] [--tasks t1,t2] "<instruction>"
 *   pi-swarm limit   (--run <runId> | --job <job.mjs>) --pool <gate|poolName> [--limit N] [--min N] [--max N] [--tpm N] [--rpm M]
 *   pi-swarm status  (--run <runId> | --job <job.mjs>)
 *   pi-swarm stop    (--run <runId> | --job <job.mjs>)
 *   pi-swarm dashboard --run <runId> [--port 3010] [--runtime-db <url>]
 */
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { runSupervisor, defaultSocketPath } from '../src/supervisor.mjs';
import { RuntimeDb } from '../src/runtime-db.mjs';
import { startDashboardServer } from '../src/dashboard/server.mjs';

function parseFlags(argv) {
  const flags = {};
  const positional = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const eq = a.indexOf('=');
      if (eq >= 0) {
        flags[a.slice(2, eq)] = a.slice(eq + 1);
      } else {
        const key = a.slice(2);
        const next = argv[i + 1];
        if (next !== undefined && !next.startsWith('--')) {
          flags[key] = next;
          i++;
        } else {
          flags[key] = true;
        }
      }
    } else {
      positional.push(a);
    }
  }
  return { flags, positional };
}

function sendControl(sockPath, payload) {
  return new Promise((resolve, reject) => {
    const client = net.createConnection(sockPath, () => {
      client.write(JSON.stringify(payload) + '\n');
    });
    let out = '';
    client.on('data', chunk => {
      out += chunk.toString('utf8');
    });
    client.on('end', () => {
      try {
        resolve(JSON.parse(out.trim()));
      } catch {
        resolve({ raw: out.trim() });
      }
    });
    client.on('error', reject);
  });
}

async function resolveTarget(flags) {
  let runId = flags.run || null;
  let dbUrl = flags['runtime-db'] || process.env.SWARM_DATABASE_URL || null;
  if (flags.job) {
    const mod = await import(pathToFileURL(path.resolve(flags.job)).href);
    const job = typeof mod.default === 'function' ? await mod.default() : mod.default;
    runId = runId || job.runId;
    dbUrl = dbUrl || job.runtimeDatabaseUrl || null;
  }
  const sock = flags.socket || (runId ? defaultSocketPath(runId) : null);
  if (!sock && !runId) {
    throw new Error('Specify --run <runId>, --job <job.mjs>, or --socket <path>');
  }
  return { runId, sock, dbUrl };
}

function formatTranscript(task) {
  const lines = [
    `Task: ${task.id}  Status: ${task.status}  Attempt: ${task.attempt}  Failures: ${task.failures}`,
    `Tokens: ${task.input_tokens} in / ${task.output_tokens} out  Cost: $${Number(task.cost_usd || 0).toFixed(6)}`,
  ];
  if (task.last_error) lines.push(`Last Error: ${task.last_error}`);
  lines.push('--- Session Transcript ---');
  const msgs = (task.entries || []).filter(e => e.type === 'message' || e.message);
  if (!msgs.length) {
    lines.push('(No session entries stored)');
  } else {
    for (const e of msgs) {
      const m = e.message || e;
      const role = m.role || e.type;
      const body = typeof m.content === 'string' ? m.content : JSON.stringify(m.content);
      lines.push(`[${role}${m.stopReason ? ':' + m.stopReason : ''}] ${body.slice(0, 1200)}`);
      if (m.errorMessage) lines.push(`  ! error: ${m.errorMessage}`);
    }
  }
  return lines.join('\n');
}

const [subcmd, ...rest] = process.argv.slice(2);
const { flags, positional } = parseFlags(rest);

if (!subcmd || subcmd === '--help' || subcmd === '-h' || subcmd === 'help') {
  console.log(`Usage:
  pi-swarm run <job.mjs> [--concurrency 3000] [--workers 30] [--sample 30] [--budget 25] [--webhook <url>] [--dashboard 3010] [--runtime-db <url>] [--only-tasks t1,t2] [--keep-alive]
  pi-swarm inspect (--run <runId> | --job <job.mjs>) <taskId> [--json] [--runtime-db <url>]
  pi-swarm retry   (--run <runId> | --job <job.mjs>) [--tasks t1,t2] [--runtime-db <url>]
  pi-swarm pause   (--run <runId> | --job <job.mjs>) [reason]
  pi-swarm resume  (--run <runId> | --job <job.mjs>)
  pi-swarm budget  (--run <runId> | --job <job.mjs>) --usd <amount|none>
  pi-swarm steer   (--run <runId> | --job <job.mjs>) [--interrupt] [--tasks t1,t2] "<instruction>"
  pi-swarm limit   (--run <runId> | --job <job.mjs>) --pool <gate|poolName> [--limit N] [--min N] [--max N] [--tpm N] [--rpm M]
  pi-swarm status  (--run <runId> | --job <job.mjs>)
  pi-swarm stop    (--run <runId> | --job <job.mjs>)
  pi-swarm dashboard --run <runId> [--port 3010] [--host 127.0.0.1] [--runtime-db <url>]`);
  process.exit(0);
}

if (subcmd === 'run') {
  const jobFile = positional[0] || flags.job;
  if (!jobFile) throw new Error('Missing <job.mjs> path');
  await runSupervisor({
    jobPath: jobFile,
    runtimeDatabaseUrl: flags['runtime-db'],
    concurrency: flags.concurrency ? Number(flags.concurrency) : undefined,
    workers: flags.workers ? Number(flags.workers) : undefined,
    sampleCount: flags.sample ? Number(flags.sample) : null,
    budgetUsd: flags.budget !== undefined ? Number(flags.budget) : undefined,
    webhookUrl: flags.webhook || undefined,
    socketPath: flags.socket,
    dashboardPort: flags.dashboard ? Number(flags.dashboard) : undefined,
    dashboardHost: flags.host || '127.0.0.1',
    onlyTasks: flags['only-tasks']
      ? String(flags['only-tasks'])
          .split(',')
          .map(s => s.trim())
          .filter(Boolean)
      : null,
    exitWhenDone: !flags['keep-alive'],
  });
} else if (subcmd === 'inspect') {
  const taskId = positional[0] || flags.task;
  if (!taskId) throw new Error('Missing <taskId> to inspect');
  const { runId, sock, dbUrl } = await resolveTarget(flags);
  let taskInfo = null;
  if (sock && fs.existsSync(sock)) {
    const res = await sendControl(sock, { action: 'inspect', taskId }).catch(() => null);
    if (res?.ok) taskInfo = res.task;
  }
  if (!taskInfo && dbUrl && runId) {
    const db = new RuntimeDb(dbUrl, { max: 2 });
    await db.migrate();
    taskInfo = await db.inspectTask(runId, taskId);
    await db.close();
  }
  if (!taskInfo) {
    console.error(`Task "${taskId}" not found.`);
    process.exit(1);
  }
  console.log(flags.json ? JSON.stringify(taskInfo, null, 2) : formatTranscript(taskInfo));
} else if (subcmd === 'retry') {
  const { runId, sock, dbUrl } = await resolveTarget(flags);
  const taskList =
    typeof flags.tasks === 'string'
      ? flags.tasks
          .split(',')
          .map(s => s.trim())
          .filter(Boolean)
      : null;
  if (sock && fs.existsSync(sock)) {
    console.log(JSON.stringify(await sendControl(sock, { action: 'retry', tasks: taskList }), null, 2));
  } else if (dbUrl && runId) {
    const db = new RuntimeDb(dbUrl, { max: 2 });
    await db.migrate();
    const resetIds = await db.retryFailedTasks(runId, taskList);
    await db.close();
    console.log(JSON.stringify({ ok: true, offline: true, retriedCount: resetIds.length, tasks: resetIds }, null, 2));
  } else {
    throw new Error('Swarm is not running and no --runtime-db / SWARM_DATABASE_URL was provided');
  }
} else if (subcmd === 'pause') {
  const { sock } = await resolveTarget(flags);
  const reason = positional.join(' ').trim() || flags.reason || 'manual_pause';
  console.log(JSON.stringify(await sendControl(sock, { action: 'pause', reason }), null, 2));
} else if (subcmd === 'resume') {
  const { sock } = await resolveTarget(flags);
  console.log(JSON.stringify(await sendControl(sock, { action: 'resume' }), null, 2));
} else if (subcmd === 'budget') {
  if (flags.usd === undefined) throw new Error('Specify --usd <amount|none>');
  const { sock } = await resolveTarget(flags);
  console.log(JSON.stringify(await sendControl(sock, { action: 'budget', usd: flags.usd }), null, 2));
} else if (subcmd === 'steer') {
  const text = positional.join(' ').trim();
  if (!text) throw new Error('Missing broadcast instruction text');
  const { sock } = await resolveTarget(flags);
  const res = await sendControl(sock, {
    action: 'steer',
    interrupt: Boolean(flags.interrupt),
    tasks:
      typeof flags.tasks === 'string'
        ? flags.tasks
            .split(',')
            .map(s => s.trim())
            .filter(Boolean)
        : null,
    text,
  });
  console.log(JSON.stringify(res, null, 2));
} else if (subcmd === 'limit') {
  if (!flags.pool) throw new Error('Missing --pool <gate|poolName>');
  const { sock } = await resolveTarget(flags);
  const res = await sendControl(sock, {
    action: 'set_limit',
    pool: flags.pool,
    limit: flags.limit !== undefined ? Number(flags.limit) : undefined,
    min: flags.min !== undefined ? Number(flags.min) : undefined,
    max: flags.max !== undefined ? Number(flags.max) : undefined,
    tpm: flags.tpm !== undefined ? Number(flags.tpm) : undefined,
    perMinute: flags.rpm !== undefined ? Number(flags.rpm) : undefined,
  });
  console.log(JSON.stringify(res, null, 2));
} else if (subcmd === 'status') {
  const { sock } = await resolveTarget(flags);
  console.log(JSON.stringify(await sendControl(sock, { action: 'status' }), null, 2));
} else if (subcmd === 'stop') {
  const { sock } = await resolveTarget(flags);
  console.log(JSON.stringify(await sendControl(sock, { action: 'stop' }), null, 2));
} else if (subcmd === 'dashboard') {
  const { runId, dbUrl } = await resolveTarget(flags);
  if (!runId) throw new Error('Missing --run <runId>');
  const runtimeDb = new RuntimeDb(dbUrl, { max: 5, applicationName: 'pi-swarm-dashboard' });
  await runtimeDb.migrate();
  const port = Number(flags.port || 3010);
  const host = flags.host || '127.0.0.1';
  await startDashboardServer({ port, host, runId, runtimeDb });
  console.log(`pi-swarm standalone dashboard on http://${host}:${port} (run=${runId})`);
} else {
  console.error(`Unknown command: ${subcmd}`);
  process.exit(1);
}
