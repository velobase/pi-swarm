# `@velobase/pi-swarm`

Task-agnostic, PostgreSQL-backed high-concurrency autonomous subagent scheduler for `@earendil-works/pi-coding-agent` (e.g., **30 Node workers × 100 async sessions = 3,000 concurrent autonomous agents**).

`@velobase/pi-swarm` owns **only** scheduling, session checkpointing, upstream rate gating, and live control—never assuming what tools, APIs, or schemas your batch job uses.

---

## Core Guarantees

1. **Zero-Loss PostgreSQL Checkpointing (Isolated `swarm_runtime` DB)**
   - Runtime tables (`swarm_runs`, `swarm_tasks`, `swarm_sessions`, `swarm_cache`, `swarm_broadcasts`) live in a dedicated runtime database (`SWARM_DATABASE_URL`), keeping your business database 100% clean.
   - **Session checkpoint & resume**: After every tool execution and model turn, `[header, ...entries]` is upserted to `swarm_sessions`. On restart or worker crash, interrupted sessions restore in-memory via `SessionManager.inMemory(cwd, {}, entries)` and continue seamlessly; completed sessions are pruned automatically.
   - **Persistent KV cache (`ctx.cache`)**: Generic `cache.get(ns, key, ttlSec)` / `cache.set(ns, key, val)` backed by `swarm_cache` so custom tools never repeat expensive external I/O after restarts.
   - **Persistent learned limits**: AIMD `gate_limit`, named pool limits, and custom job `metrics` persist in `swarm_runs` across restarts.

2. **Adaptive Upstream AIMD Gate + Priority Retry Queue**
   - Probes upstream LLM capacity dynamically via Additive-Increase / Multiplicative-Decrease (`Gate`), with a single decrease per cooldown window.
   - **Priority retry queue**: Retried turns jump ahead of cold first-attempt requests so active sessions never starve behind queued newcomers.
   - **30-minute HTTP idle timeout** (`httpIdleTimeoutMs: 1_800_000`) prevents local queue waiting from triggering Node `undici` 300s self-abort loops.
   - **Pluggable auth & `401` self-healing**: Supports `$ENV_VAR`, custom `{ getToken, invalidate }`, or `gcloud-vertex` (queries real OAuth TTL and transparently refreshes + retries once inside the same slot on HTTP `401`).

3. **Generic Named Resource Pools (`ctx.pool`) & Metrics (`ctx.metric`)**
   - Zero hardcoded external tools. Declare any external resource caps your job needs in `pools: { [name]: { limit, perMinute } }` and wrap calls in `ctx.pool(name, fn)` inside your custom tools.
   - Emit arbitrary job counters via `ctx.metric(name, delta)`—automatically aggregated and rendered on the live dashboard.

4. **Live Control Plane & Zero-Dependency SSE Dashboard**
   - `pi-swarm steer`: Millisecond broadcast to all live sessions (`--interrupt` to abort slow streaming turns immediately) with delivery & consumption receipts.
   - `pi-swarm limit`: Hot-tune `gate` or any named `<poolName>` concurrency/RPM online without restarting.
   - Built-in real-time SSE telemetry dashboard (`--dashboard 3010`).

---

## Installation

```bash
npm install @velobase/pi-swarm
# Or clone globally for CLI usage:
git clone https://github.com/velobase/pi-swarm.git
cd pi-swarm && npm install && npm link
```

---

## Quick Start

```js
import { defineSwarm, parseMaybeJson, fuzzyResolveId } from '@velobase/pi-swarm';

export default defineSwarm({
  runId: 'my-batch-job-v1',
  name: 'My 3,000-Agent Batch Job',
  concurrency: 3000,
  workers: 30,
  model: {
    provider: 'openai-compat',
    id: 'deepseek-chat',
    baseUrl: 'https://api.deepseek.com/v1',
    apiKey: '$DEEPSEEK_API_KEY',
  },
  gate: { limit: 300, min: 50, max: 1500 },
  // Optional: declare named concurrency / RPM pools for external APIs your tools call
  pools: {
    githubApi: { limit: 80, perMinute: 4500 },
  },

  async tasks() {
    // Return [{ id, payload }]
  },

  async loadPending(task) {
    // Query business DB for remaining items; returning [] marks task complete
  },

  systemPrompt: 'You are an autonomous worker...',
  buildPrompt: (_task, pending) => `Process items:\n${JSON.stringify(pending)}`,

  tools: (task, { Type, pool, cache, metric, nativeFetch }) => [
    {
      name: 'lookup_repo',
      label: 'Fetch repo metadata',
      description: 'Fetch metadata with global pooling and PG cache.',
      parameters: Type.Object({ repo: Type.String() }),
      execute: async (_callId, { repo }) => {
        const cached = await cache.get('gh', repo, 86_400);
        if (cached !== undefined) return { content: [{ type: 'text', text: JSON.stringify(cached) }] };
        const res = await pool('githubApi', async () => {
          const r = await nativeFetch(`https://api.github.com/repos/${repo}`);
          return r.json();
        });
        metric('gh_calls', 1);
        await cache.set('gh', repo, res);
        return { content: [{ type: 'text', text: JSON.stringify(res) }] };
      },
    },
    {
      name: 'save_output',
      label: 'Persist output to business DB',
      description: 'Write structured result.',
      parameters: Type.Object({ id: Type.String(), result: Type.Any() }),
      execute: async (_callId, raw) => {
        const args = parseMaybeJson(raw);
        // Write to business DB...
        metric('saved', 1);
        return { content: [{ type: 'text', text: '{"ok":true}' }] };
      },
    },
  ],
});
```

### CLI Operations

```bash
pi-swarm run ./job.mjs --dashboard 3010
pi-swarm steer --job ./job.mjs "Updated instruction for all active agents"
pi-swarm steer --job ./job.mjs --interrupt "Abort slow turns and apply new rule immediately"
pi-swarm limit --job ./job.mjs --pool gate --limit 1200 --max 1500
pi-swarm limit --job ./job.mjs --pool githubApi --limit 150 --rpm 4800
pi-swarm status --job ./job.mjs
```

---

## License

MIT © Velobase
