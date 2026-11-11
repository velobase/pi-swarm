# `@velobase/pi-swarm`

PostgreSQL-backed high-concurrency autonomous subagent orchestrator for `@earendil-works/pi-coding-agent` (e.g., **30 Node workers × 100 async sessions = 3,000 concurrent autonomous agents**).

Built from real-world 30,000+ item production crawls, multilingual copy-rewrites, and catalog audits—eliminating cold-restart context loss, local DNS/conntrack exhaustion, OAuth token expiry storms, and upstream rate-limit queue starvation.

---

## Why `pi-swarm`?

Running thousands of autonomous LLM sessions concurrently exposes failure modes that ad-hoc scripts miss:

1. **Zero-Loss PostgreSQL Checkpointing (Strict Runtime/Business DB Isolation)**
   - Runtime state lives in a dedicated `swarm_runtime` PostgreSQL database (`swarm_runs`, `swarm_tasks`, `swarm_sessions`, `swarm_fetch_cache`, `swarm_broadcasts`), keeping your production/business database 100% clean for `pg_dump`.
   - **No lost web fetches**: Built-in `webfetch` (`curl` + Chrome UA) and `jina_read` (`r.jina.ai`) cache responses in `swarm_fetch_cache`. Restarts never re-crawl or re-burn Jina tokens.
   - **No lost in-flight conversations**: After every tool execution and model turn, `[header, ...entries]` is persisted to `swarm_sessions`. On restart, interrupted sessions restore seamlessly via `SessionManager.inMemory(cwd, {}, entries)` and continue where they stopped.
   - **No lost AIMD rate-limit tuning**: Learned concurrency limits (`gate_limit`) and pool settings persist in `swarm_runs` across restarts.

2. **Adaptive Upstream AIMD Gate + Priority Retry Queue**
   - Dynamically probes upstream capacity (e.g., Google Vertex AI Dynamic Shared Quota) via Additive-Increase / Multiplicative-Decrease (`Gate`), with a single decrease per cooldown window so burst `429`s never collapse throughput.
   - **Priority retry queue**: Retried turns jump ahead of cold first-attempt requests so long-running sessions never starve behind 2,900 queued newcomers.
   - **30-minute HTTP idle timeout** (`httpIdleTimeoutMs: 1_800_000`) prevents local queue wait time from triggering Node `undici` 300s self-abort loops.
   - **Transparent OAuth / `401` self-healing**: Built-in `gcloud-vertex` auth checks real token TTL via `oauth2.googleapis.com/tokeninfo` and transparently refreshes + retries once inside the same slot on HTTP `401`.

3. **Bounded External Tool Pools (`SlotPool`)**
   - Central Supervisor FIFO + sliding-window RPM limiters for external I/O (`curl` concurrency cap to protect local DNS/conntrack; `jina` concurrency + 480 RPM cap to respect API rate limits).

4. **Live Control Plane & Zero-Dependency SSE Dashboard**
   - `pi-swarm steer`: Millisecond broadcast to all 3,000 live sessions (`--interrupt` to abort slow turns and redirect immediately) with delivery & consumption receipts.
   - `pi-swarm limit`: Hot-tune `gate`, `curl`, or `jina` concurrency/RPM limits online without restarting workers.
   - Built-in real-time dark/light SSE telemetry dashboard (`--dashboard 3010`).

---

## Installation

```bash
npm install @velobase/pi-swarm
# Or clone globally for CLI usage:
git clone https://github.com/velobase/pi-swarm.git
cd pi-swarm && npm install && npm link
```

Requires Node.js `>= 20`, PostgreSQL `>= 14`, and `@earendil-works/pi-coding-agent`.

---

## Quick Start

### 1. Provision an isolated local runtime database

Keep runtime checkpoints separate from your business database:

```bash
sudo -u postgres psql -p 15432 -c "CREATE USER swarm_runtime WITH PASSWORD 'local_swarm_pw';"
sudo -u postgres psql -p 15432 -c "CREATE DATABASE swarm_runtime OWNER swarm_runtime;"
export SWARM_DATABASE_URL="postgresql://swarm_runtime:local_swarm_pw@127.0.0.1:15432/swarm_runtime"
```

### 2. Define a job in a single file (`job.mjs`)

```js
import { defineSwarm, parseMaybeJson, fuzzyResolveId } from '@velobase/pi-swarm';

export default defineSwarm({
  runId: 'catalog-audit-2026',
  name: 'Catalog Audit & 10-Locale Copy Rewrite',
  concurrency: 3000,
  workers: 30,
  model: {
    provider: 'vertex-ds',
    id: 'deepseek-ai/deepseek-v4.1-flash-maas',
    vertexProject: 'my-gcp-project',
    vertexLocation: 'global',
    auth: 'gcloud-vertex',
  },
  gate: { limit: 300, min: 50, max: 1500 },
  pools: {
    curl: { limit: 300 },
    jina: { limit: 300, perMinute: 480 },
  },

  async tasks() {
    // Return [{ id, payload }]
  },

  async loadPending(task) {
    // Query business DB for remaining items in this task; returning [] marks task complete
  },

  systemPrompt: 'You are an autonomous auditor...',
  buildPrompt: (task, pending) => `Process remaining items:\n${JSON.stringify(pending)}`,

  tools: (task, { Type, builtins }) => [
    builtins.webfetch,
    builtins.jinaRead,
    {
      name: 'save_item',
      label: 'Save item to business PostgreSQL',
      description: 'Atomically persist structured output.',
      parameters: Type.Object({ id: Type.String(), data: Type.Any() }),
      execute: async (_callId, raw) => {
        const args = parseMaybeJson(raw); // Liberal JSON normalization built in
        // Write to your business PostgreSQL pool...
        return { content: [{ type: 'text', text: '{"ok":true}' }] };
      },
    },
  ],
});
```

### 3. Run & Operate

```bash
# Start 3,000 concurrent sessions + live SSE dashboard on port 3010
pi-swarm run ./job.mjs --dashboard 3010

# Steer all live sessions without restarting
pi-swarm steer --job ./job.mjs "Skip unreachable .onion domains and keep existing metadata."

# Interrupt slow streaming sessions immediately with new guidance
pi-swarm steer --job ./job.mjs --interrupt "Prioritize concise 2-paragraph overviews."

# Hot-adjust gate or tool pool limits online
pi-swarm limit --job ./job.mjs --pool gate --limit 1200 --max 1500
pi-swarm limit --job ./job.mjs --pool curl --limit 400

# Inspect live JSON status
pi-swarm status --job ./job.mjs
```

---

## License

MIT © Velobase
