/**
 * Swarm Worker process: hosts up to N concurrent Pi AgentSessions,
 * snapshots session state to PostgreSQL (`swarm_sessions`) after every turn,
 * seamlessly resumes interrupted sessions on restart, and routes all LLM/curl/Jina calls through Supervisor gates.
 */
import { AsyncLocalStorage } from 'node:async_hooks';
import { pathToFileURL } from 'node:url';
import {
  installGatedFetch,
  makeSlotClient,
  createGcloudVertexAuth,
  createStaticAuth,
} from './gate.mjs';
import { RuntimeDb } from './runtime-db.mjs';
import { createBuiltinTools } from './builtin-tools.mjs';
import { loadPiSdk, createJobModelRuntime } from './sdk-loader.mjs';
import { parseMaybeJson } from './index.mjs';

process.umask(0o077);

const jobPath = process.env.PI_SWARM_JOB;
const runtimeDbUrl = process.env.SWARM_DATABASE_URL;
const shardIndex = Number(process.env.PI_SHARD || 0);

if (!jobPath) throw new Error('Worker missing PI_SWARM_JOB');

const jobMod = await import(pathToFileURL(jobPath).href);
const job = typeof jobMod.default === 'function' ? await jobMod.default() : jobMod.default;

const local = new AsyncLocalStorage();
const running = new Map(); // taskId -> { session, sessionManager, task }
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
const auth =
  job.model.auth === 'gcloud-vertex'
    ? createGcloudVertexAuth({ nativeFetch: rawNativeFetch })
    : typeof job.model.auth === 'object' && job.model.auth
      ? job.model.auth
      : job.model.apiKey
        ? createStaticAuth(job.model.apiKey)
        : null;

const matchUrl =
  job.model.matchUrl ||
  (job.model.baseUrl
    ? new URL(job.model.baseUrl).hostname
    : job.model.vertexProject
      ? 'aiplatform.googleapis.com'
      : null);

const nativeFetch = installGatedFetch({
  send,
  matchUrl,
  auth,
  getTaskContext: () => local.getStore(),
  onEvent: e => {
    const task = local.getStore();
    if (task && e.type === 'llm_status') event(task, 'http_headers', { status: e.status });
  },
});

const withCurlSlot = makeSlotClient('curl', send);
const withJinaSlot = makeSlotClient('jina', send);

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
      pending = await job.loadPending(task, { runtimeDb });
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

    const builtins = createBuiltinTools({
      Type,
      runtimeDb,
      withCurlSlot,
      withJinaSlot,
      nativeFetch,
      send,
      cacheTtlSec: job.fetchCacheTtlSec ?? 86_400,
      jinaApiKey: job.jinaApiKey ?? null,
    });

    const rawTools =
      typeof job.tools === 'function' ? await job.tools(task, { Type, builtins, runtimeDb }) : [];
    const customTools = rawTools.map(wrapUserTool);

    const savedEntries = await runtimeDb.loadSession(job.runId, id);
    const isResuming = Array.isArray(savedEntries) && savedEntries.length > 1;
    manager = isResuming
      ? SessionManager.inMemory(job.cwd, {}, savedEntries)
      : SessionManager.inMemory(job.cwd);

    const sysPrompt =
      typeof job.systemPrompt === 'function' ? await job.systemPrompt(task) : job.systemPrompt;

    const created = await createAgentSession({
      cwd: job.cwd,
      agentDir,
      modelRuntime,
      model,
      sessionManager: manager,
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

    session = created.session;
    running.set(id, { session, sessionManager: manager, task });
    event(id, 'session_ready', { model: session.model?.id, resumed: isResuming });

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

    const promptText = isResuming
      ? (typeof job.buildResumePrompt === 'function'
          ? await job.buildResumePrompt(task, pending)
          : `Session restored from PostgreSQL checkpoint after worker restart. Remaining unfinished items:\n${JSON.stringify(pending)}\nContinue from where you stopped without repeating completed items.`) +
        bcBlock
      : (await job.buildPrompt(task, pending)) + bcBlock;

    await local.run(id, () => session.prompt(promptText, { expandPromptTemplates: false }));

    let continuations = 0;
    const maxContinuations = job.maxContinuations ?? 5;
    let last = session.messages.filter(m => m.role === 'assistant').at(-1);
    while (last?.stopReason === 'error' && continuations < maxContinuations && !stopping) {
      continuations++;
      await new Promise(r => setTimeout(r, 10_000 * continuations));
      if (stopping) break;
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

    event(id, 'run_finished', {
      error: last?.stopReason === 'error' ? last?.errorMessage : undefined,
      stopReason: last?.stopReason,
      continuations,
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
