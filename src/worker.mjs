/**
 * Swarm Worker process: task-agnostic host for up to N concurrent Pi AgentSessions.
 * Snapshots session state to PostgreSQL (`swarm_sessions`) after every turn,
 * reports token usage & liveness activity, supports multi-endpoint/fallback routing,
 * and exposes `ctx.pool(name, fn)`, `ctx.cache`, and `ctx.metric(name, delta)` primitives.
 */
import { AsyncLocalStorage } from 'node:async_hooks';
import { pathToFileURL } from 'node:url';
import {
  installGatedFetch,
  makeSlotClient,
  EndpointRouter,
  resolveAuthSpec,
} from './gate.mjs';
import { RuntimeDb } from './runtime-db.mjs';
import { loadPiSdk, createJobModelRuntime } from './sdk-loader.mjs';
import { parseMaybeJson } from './index.mjs';

process.umask(0o077);

const jobPath = process.env.PI_SWARM_JOB;
const runtimeDbUrl = process.env.SWARM_DATABASE_URL;
const shardIndex = Number(process.env.PI_SHARD || 0);
const forceRetainSessions = process.env.PI_SWARM_RETAIN_SESSIONS === '1';

if (!jobPath) throw new Error('Worker missing PI_SWARM_JOB');

const jobMod = await import(pathToFileURL(jobPath).href);
const job = typeof jobMod.default === 'function' ? await jobMod.default() : jobMod.default;

const local = new AsyncLocalStorage();
const running = new Map(); // taskId -> { session, sessionManager, task, abortedReason }
let stopping = false;

const send = data => {
  if (process.connected) process.send(data);
};
const event = (task, type, data = {}) => send({ task, type, ...data });

const runtimeDb = new RuntimeDb(runtimeDbUrl || job.runtimeDatabaseUrl, {
  max: Number(process.env.PI_SWARM_RUNTIME_POOL_MAX || 10),
  applicationName: `pi-swarm-worker-${shardIndex}`,
});

const rawNativeFetch = globalThis.fetch;
const primaryBaseUrl =
  job.model.baseUrl ||
  (job.model.vertexProject
    ? `https://aiplatform.googleapis.com/v1/projects/${job.model.vertexProject}/locations/${job.model.vertexLocation || 'global'}/endpoints/openapi`
    : null);

const router =
  Array.isArray(job.model.endpoints) && job.model.endpoints.length > 0
    ? new EndpointRouter(job.model.endpoints, { nativeFetch: rawNativeFetch })
    : null;

const auth = resolveAuthSpec(job.model.auth || job.model.apiKey, { nativeFetch: rawNativeFetch });

const matchUrl =
  job.model.matchUrl ||
  (primaryBaseUrl
    ? url => {
        try {
          const u = new URL(url);
          const p = new URL(primaryBaseUrl);
          return u.host === p.host && u.pathname.startsWith(p.pathname.replace(/\/+$/, ''));
        } catch {
          return false;
        }
      }
    : null);

const nativeFetch = installGatedFetch({
  send,
  matchUrl,
  primaryBaseUrl,
  auth,
  router,
  getTaskContext: () => local.getStore(),
  onEvent: e => {
    const task = local.getStore();
    if (task && e.type === 'llm_status') {
      event(task, 'http_headers', { status: e.status, endpoint: e.endpoint });
    }
  },
});

const slotClients = new Map();
function withNamedPool(name, fn) {
  let client = slotClients.get(name);
  if (!client) {
    client = makeSlotClient(String(name), send);
    slotClients.set(name, client);
  }
  return client(fn);
}

const cacheHelper = {
  get: (namespace, key, ttlSec = 0) => runtimeDb.cacheGet(namespace, key, ttlSec),
  set: (namespace, key, value) => runtimeDb.cacheSet(namespace, key, value),
};

function reportMetric(name, delta = 1) {
  send({ type: 'metric', name: String(name), delta: Number(delta) || 0 });
}

const { sdk, Type } = await loadPiSdk();
const { createAgentSession, SessionManager, SettingsManager, createExtensionRuntime } = sdk;
const { modelRuntime, model, agentDir } = await createJobModelRuntime(sdk, job.model);

if (typeof job.setupWorker === 'function') {
  await job.setupWorker({ shardIndex, runtimeDb });
}

function makeResourceLoader(systemPrompt) {
  return {
    getExtensions: () => ({ extensions: [], errors: [], runtime: createExtensionRuntime() }),
    getSkills: () => ({ skills: [], diagnostics: [] }),
    getPrompts: () => ({ prompts: [], diagnostics: [] }),
    getThemes: () => ({ themes: [], diagnostics: [] }),
    getAgentsFiles: () => ({ agentsFiles: [] }),
    getSystemPrompt: () => String(systemPrompt || ''),
    getSystemPromptSource: () => undefined,
    getAppendSystemPrompt: () => [],
    getAppendSystemPromptSources: () => [],
    extendResources: () => {},
    reload: async () => {},
  };
}

function serializeSessionManager(manager) {
  const header = manager.getHeader?.();
  const entries = manager.getEntries?.() || [];
  return header ? [header, ...entries] : entries;
}

function wrapUserTool(tool) {
  return {
    ...tool,
    execute: async (callId, rawArgs, signal) => {
      const normalized = parseMaybeJson(rawArgs);
      return tool.execute(callId, normalized, signal);
    },
  };
}

const sharedCtx = {
  Type,
  pool: withNamedPool,
  cache: cacheHelper,
  metric: reportMetric,
  nativeFetch,
  runtimeDb,
};

async function runTask(task, attempt, pendingBroadcasts = []) {
  const id = String(task.id);
  let session;
  let manager;
  let checkpointChain = Promise.resolve();

  const queueCheckpoint = () => {
    if (!manager) return checkpointChain;
    const snap = serializeSessionManager(manager);
    checkpointChain = checkpointChain
      .then(() => runtimeDb.saveSession(job.runId, id, snap))
      .catch(() => {});
    return checkpointChain;
  };

  try {
    let pending = task.payload;
    if (typeof job.loadPending === 'function') {
      pending = await job.loadPending(task, sharedCtx);
      if (
        pending === null ||
        pending === undefined ||
        pending?.complete === true ||
        (Array.isArray(pending) && pending.length === 0)
      ) {
        event(id, 'run_finished', { stopReason: 'already_complete' });
        return;
      }
    }

    const rawTools =
      typeof job.tools === 'function' ? await job.tools(task, sharedCtx) : [];
    const customTools = rawTools.map(wrapUserTool);

    const savedEntries = await runtimeDb.loadSession(job.runId, id);
    const isResuming = Array.isArray(savedEntries) && savedEntries.length > 1;
    let isResumingSafe = isResuming;
    try {
      manager = isResuming
        ? SessionManager.inMemory(job.cwd, {}, savedEntries)
        : SessionManager.inMemory(job.cwd);
    } catch {
      isResumingSafe = false;
      manager = SessionManager.inMemory(job.cwd);
    }

    const sysPrompt =
      typeof job.systemPrompt === 'function'
        ? await job.systemPrompt(task, sharedCtx)
        : job.systemPrompt;

    const buildSession = mgr =>
      createAgentSession({
        cwd: job.cwd,
        agentDir,
        modelRuntime,
        model,
        sessionManager: mgr,
        settingsManager: SettingsManager.inMemory({
          httpIdleTimeoutMs: job.httpIdleTimeoutMs ?? 30 * 60_000,
          compaction: job.compaction ?? {
            enabled: true,
            reserveTokens: 20_000,
            keepRecentTokens: 25_000,
          },
          retry: job.retry,
          cacheWarming: 'off',
        }),
        resourceLoader: makeResourceLoader(sysPrompt),
        tools: customTools.map(t => t.name),
        customTools,
      });

    let created;
    try {
      created = await buildSession(manager);
    } catch (err) {
      if (!isResumingSafe) throw err;
      isResumingSafe = false;
      manager = SessionManager.inMemory(job.cwd);
      created = await buildSession(manager);
    }

    session = created.session;
    const entryObj = { session, sessionManager: manager, task, abortedReason: null };
    running.set(id, entryObj);
    event(id, 'session_ready', { model: session.model?.id, resumed: isResumingSafe });

    session.subscribe(e => {
      if (e.type === 'agent_start') event(id, 'agent_start');
      else if (e.type === 'agent_end') event(id, 'agent_end', { willRetry: e.willRetry });
      else if (e.type === 'auto_retry_start') {
        event(id, 'retry', { error: e.errorMessage, delay: e.delayMs });
      } else if (e.type === 'tool_execution_start') {
        event(id, 'tool_start', { tool: e.toolName });
      } else if (e.type === 'tool_execution_end') {
        void queueCheckpoint();
        event(id, 'tool_end', {
          tool: e.toolName,
          isError: e.isError,
          error: e.isError ? JSON.stringify(e.result?.content).slice(0, 500) : undefined,
        });
      } else if (e.type === 'message_end') {
        void queueCheckpoint();
        if (e.message?.role === 'assistant' && e.message?.usage) {
          const u = e.message.usage;
          event(id, 'usage', {
            input: Number(u.input || u.prompt_tokens || 0),
            output: Number(u.output || u.completion_tokens || 0),
            cacheRead: Number(u.cacheRead || 0),
            cacheWrite: Number(u.cacheWrite || 0),
          });
        }
      } else if (e.type === 'message_start' && e.message.role === 'user') {
        const text = JSON.stringify(e.message.content || '');
        const matches = [...text.matchAll(/\[BROADCAST:([A-Za-z0-9_-]+)\]/g)];
        for (const m of matches) {
          send({ type: 'broadcast_consumed', id: m[1], task: id, pid: process.pid });
        }
      }
    });

    const bcBlock = pendingBroadcasts.length
      ? '\n' + pendingBroadcasts.map(b => `[BROADCAST:${b.id}]\n${b.text}`).join('\n')
      : '';

    const promptText = isResumingSafe
      ? (typeof job.buildResumePrompt === 'function'
          ? await job.buildResumePrompt(task, pending, sharedCtx)
          : `Session restored from PostgreSQL checkpoint after worker restart. Remaining unfinished items:\n${JSON.stringify(pending)}\nContinue from where you stopped without repeating completed items.`) +
        bcBlock
      : (await job.buildPrompt(task, pending, sharedCtx)) + bcBlock;

    await local.run(id, () => session.prompt(promptText, { expandPromptTemplates: false }));

    let continuations = 0;
    const maxContinuations = job.maxContinuations ?? 5;
    let last = session.messages.filter(m => m.role === 'assistant').at(-1);
    while (
      last?.stopReason === 'error' &&
      continuations < maxContinuations &&
      !stopping &&
      !entryObj.abortedReason
    ) {
      continuations++;
      await new Promise(r => setTimeout(r, Math.min(30_000, 2000 * continuations)));
      if (stopping || entryObj.abortedReason) break;
      event(id, 'continuation', { continuations, error: last.errorMessage });
      await local.run(id, () =>
        session.prompt(
          'Previous model turn was interrupted by an upstream error. Continue from where you stopped.',
          { expandPromptTemplates: false }
        )
      );
      last = session.messages.filter(m => m.role === 'assistant').at(-1);
    }

    await checkpointChain;

    if (stopping) {
      return;
    }

    const finalError =
      entryObj.abortedReason ||
      (last?.stopReason === 'error' ? last?.errorMessage || 'assistant_stop_error' : undefined);

    event(id, 'run_finished', {
      error: finalError,
      stopReason: entryObj.abortedReason ? 'stalled' : last?.stopReason,
      continuations,
      retainSession: forceRetainSessions,
    });
  } catch (err) {
    await checkpointChain;
    if (!stopping) {
      event(id, 'run_finished', { error: err.stack || err.message });
    }
  } finally {
    running.delete(id);
    session?.dispose();
  }
}

async function handleAbortTask({ taskId, reason }) {
  const entry = running.get(String(taskId));
  if (!entry) return;
  entry.abortedReason = reason || 'stalled_task_aborted';
  try {
    await entry.session.abort();
    const snap = serializeSessionManager(entry.sessionManager);
    await runtimeDb.saveSession(job.runId, String(taskId), snap);
  } catch {}
}

async function handleSteer(msg) {
  const { id, text, interrupt = false, tasks: filterTasks } = msg;
  const filter = Array.isArray(filterTasks) && filterTasks.length > 0 ? new Set(filterTasks) : null;
  const tagged = `[BROADCAST:${id}]\n${text}`;
  let matched = 0;
  let queuedSteer = 0;
  let interruptedAndSteered = 0;
  let startedImmediately = 0;
  const errors = [];

  await Promise.allSettled(
    [...running.entries()].map(async ([taskId, entry]) => {
      if (filter && !filter.has(taskId)) return;
      matched++;
      const { session } = entry;
      try {
        if (interrupt && session.isStreaming) {
          await session.abort();
          await session.waitForIdle();
          await local.run(taskId, () =>
            session.prompt(tagged, { expandPromptTemplates: false, streamingBehavior: 'steer' })
          );
          interruptedAndSteered++;
        } else {
          const wasStreaming = session.isStreaming;
          await local.run(taskId, () =>
            session.prompt(tagged, { expandPromptTemplates: false, streamingBehavior: 'steer' })
          );
          if (wasStreaming) queuedSteer++;
          else startedImmediately++;
        }
      } catch (err) {
        errors.push({ task: taskId, error: err.message });
      }
    })
  );

  send({
    type: 'steer_ack',
    id,
    worker: shardIndex,
    pid: process.pid,
    matched,
    queuedSteer,
    interruptedAndSteered,
    startedImmediately,
    errors,
  });
}

async function stop() {
  if (stopping) return;
  stopping = true;
  await Promise.allSettled(
    [...running.entries()].map(async ([taskId, { session, sessionManager }]) => {
      try {
        await session.abort();
        const snap = serializeSessionManager(sessionManager);
        await runtimeDb.saveSession(job.runId, taskId, snap);
      } catch {}
    })
  );
  if (typeof job.teardownWorker === 'function') {
    await job.teardownWorker({ shardIndex, runtimeDb }).catch(() => {});
  }
  await runtimeDb.close();
  process.exit(0);
}

process.on('message', message => {
  if (message.type === 'start' && !stopping) {
    void runTask(message.task, message.attempt, message.broadcasts || []);
  } else if (message.type === 'abort_task' && !stopping) {
    void handleAbortTask(message);
  } else if (message.type === 'router_stats_req') {
    send({ type: 'router_stats', shard: shardIndex, endpoints: router ? router.snapshot() : [] });
  } else if (message.type === 'steer' && !stopping) {
    void handleSteer(message);
  } else if (message.type === 'stop') {
    void stop();
  }
});

if (process.connected) {
  process.on('SIGTERM', () => void stop());
  process.on('SIGINT', () => void stop());
  process.on('disconnect', () => void stop());
}

send({ type: 'ready', pid: process.pid, shard: shardIndex, model: model.id });
