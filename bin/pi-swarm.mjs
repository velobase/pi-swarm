#!/usr/bin/env node
/**
 * CLI for @velobase/pi-swarm:
 *   pi-swarm run <job.mjs> [--concurrency 3000] [--workers 30] [--dashboard 3010] [--runtime-db postgres://...]
 *   pi-swarm steer --run <runId> [--interrupt] [--tasks t1,t2] "<instruction>"
 *   pi-swarm limit --run <runId> --pool <gate|poolName> [--limit N] [--min N] [--max N] [--rpm M]
 *   pi-swarm status --run <runId>
 *   pi-swarm stop --run <runId>
 *   pi-swarm dashboard --run <runId> [--port 3010] [--runtime-db postgres://...]
 */
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

async function resolveSocket(flags) {
  if (flags.socket) return flags.socket;
  if (flags.run) return defaultSocketPath(flags.run);
  if (flags.job) {
    const mod = await import(pathToFileURL(path.resolve(flags.job)).href);
    const job = typeof mod.default === 'function' ? await mod.default() : mod.default;
    return defaultSocketPath(job.runId);
  }
  throw new Error('Specify --run <runId>, --job <job.mjs>, or --socket <path>');
}

const [subcmd, ...rest] = process.argv.slice(2);
const { flags, positional } = parseFlags(rest);

if (!subcmd || subcmd === '--help' || subcmd === '-h' || subcmd === 'help') {
  console.log(`Usage:
  pi-swarm run <job.mjs> [--concurrency 3000] [--workers 30] [--dashboard 3010] [--runtime-db <url>] [--only-tasks t1,t2] [--keep-alive]
  pi-swarm steer (--run <runId> | --job <job.mjs>) [--interrupt] [--tasks t1,t2] "<instruction>"
  pi-swarm limit (--run <runId> | --job <job.mjs>) --pool <gate|poolName> [--limit N] [--min N] [--max N] [--rpm M]
  pi-swarm status (--run <runId> | --job <job.mjs>)
  pi-swarm stop (--run <runId> | --job <job.mjs>)
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
} else if (subcmd === 'steer') {
  const text = positional.join(' ').trim();
  if (!text) throw new Error('Missing broadcast instruction text');
  const sock = await resolveSocket(flags);
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
  const sock = await resolveSocket(flags);
  const res = await sendControl(sock, {
    action: 'set_limit',
    pool: flags.pool,
    limit: flags.limit !== undefined ? Number(flags.limit) : undefined,
    min: flags.min !== undefined ? Number(flags.min) : undefined,
    max: flags.max !== undefined ? Number(flags.max) : undefined,
    perMinute: flags.rpm !== undefined ? Number(flags.rpm) : undefined,
  });
  console.log(JSON.stringify(res, null, 2));
} else if (subcmd === 'status') {
  const sock = await resolveSocket(flags);
  const res = await sendControl(sock, { action: 'status' });
  console.log(JSON.stringify(res, null, 2));
} else if (subcmd === 'stop') {
  const sock = await resolveSocket(flags);
  const res = await sendControl(sock, { action: 'stop' });
  console.log(JSON.stringify(res, null, 2));
} else if (subcmd === 'dashboard') {
  const runId = flags.run;
  if (!runId) throw new Error('Missing --run <runId>');
  const dbUrl = flags['runtime-db'] || process.env.SWARM_DATABASE_URL;
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
