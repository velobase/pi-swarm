---
name: pi-swarm
description: Run hundreds to thousands of autonomous Pi agents over a batch of work (research, audits, rewrites, migrations, data cleanup) with the pi-swarm scheduler. Use when a task has many independent items that each need an agent's judgment, and you need it to survive rate limits, restarts and long runtimes. Covers how to write the job file, how to launch and watch a run, and the failure modes to design against.
---

# Running an agent swarm with pi-swarm

pi-swarm runs N worker processes, each hosting many Pi agent sessions, and takes care of the parts that are the same for every batch job: admission to the model endpoint, checkpoints in PostgreSQL, named pools for external resources, live control and a dashboard. You write one `job.mjs` that says what the work is.

Use a swarm when each item needs judgment (read a page, decide, write something). If a fixed rule can do the item, write a plain script instead; it is cheaper and easier to trust.

## Division of work

| pi-swarm does | Your `job.mjs` does |
|---|---|
| Worker processes and sessions | Split the work into tasks: `tasks()` |
| Model request gate (adaptive limit, retries first, token refresh) | Say what is still open for a task: `loadPending(task)` |
| Session checkpoint and resume, key-value cache, saved limits | Prompts: `systemPrompt`, `buildPrompt`, optional `buildResumePrompt` |
| Named pools with concurrency and per-minute caps: `ctx.pool` | Tools the agent may call: `tools(task, ctx)` |
| `steer`, `limit`, `status`, `stop`, dashboard | Optional dashboard hooks: `progress`, `dashboardPanels`, `metricsMeta`, `alerts` |

The scheduler knows nothing about your task. Web fetching, API calls and database writes are tools you define in the job.

## Setup

Two databases, never one:

- **Runtime database** (`SWARM_DATABASE_URL`): tasks, session checkpoints, cache, saved limits. Local only. Safe to drop after the run.
- **Business database**: only your results. Written by your tools.

Keeping them apart means a restart never loses work and the business database can be exported without scheduler tables in it. Do not point thousands of agents at a production database; run against a local copy and sync the result afterwards.

```bash
createdb swarm_runtime
export SWARM_DATABASE_URL=postgresql://user:pass@127.0.0.1:5432/swarm_runtime
pi-swarm run ./job.mjs --dashboard 3010
```

## Writing the job

See `examples/batch-job.mjs` for a complete file. The parts that matter:

**Tasks are small and re-derivable.** A task is a handful of items (about ten). `loadPending(task)` asks the business database which of them are still open and returns `[]` when none are, which marks the task complete. Because completion is read from the results, a restarted or repeated task only does what is left.

**Tools write results directly.** Give the agent in-process tools that validate and write inside a transaction. Do not give batch agents a shell or file tools to write their own scripts; that produces thousands of stray processes and files.

**Save each item as soon as it is done**, not at the end of the task. Then an interruption costs one item, not ten.

**Describe tool parameters exactly.** Use `Type.Object({...})` with named fields for object parameters. With `Type.Any()` the model cannot see what to send and will probe with an empty object first, which costs a round per item.

**Be liberal in what tools accept.** Models under load send stringified JSON, arrays where you expected maps, and slightly wrong ids. Use `parseMaybeJson`, `normalizeKeyedMap` and `fuzzyResolveId`; bind ids to the task's own list. When input really is unusable, throw a message that names the valid values so the agent can correct itself.

**Plan for the output limit.** If one item needs a very long answer (for example the same copy in ten languages), offer a second tool that adds the rest later, and say so in the prompt. A tool call cut off by the output limit is wasted.

**Put every external resource behind a pool.** Declare it in `pools` and wrap the call in `ctx.pool(name, fn)`. Cache the result with `ctx.cache` so a restart does not repeat it. Count it with `ctx.metric`.

**Trust the agent's judgment, and make its actions reversible.** Do not wrap the agent in a loop that re-prompts until a checklist passes; that traps sessions in endless rework. A turn that ends normally completes the task. In exchange, make risky actions cheap to undo: mark rows as deleted instead of deleting them, and store the reason and evidence the agent gives.

## Lessons from production runs

Each of these cost real time once. The scheduler now handles the ones marked "built in"; the rest are yours to get right in the job.

| What went wrong | Why | What to do |
|---|---|---|
| Thousands of requests failed with 429 at start | Session count was used as request concurrency | Built in: sessions wait for a slot from the gate. Keep sessions high, let the gate find the request rate |
| Throughput collapsed to almost zero after a burst of 429s | The limit fell to a very low floor; queue wait passed the client timeout; aborted requests retried from the back of the queue, forever | Built in: 30-minute request timeout, retried requests go first. Yours: set `gate.min` to a rate you know works, not a token value |
| An hour of nothing but 401 errors | The cached access token expired; the CLI that issues it returns its own cached token, so "refresh every 30 minutes" did not help | Built in for `gcloud-vertex`: reads the token's real expiry, and on 401 refreshes and retries once. For custom auth, implement `getToken` and `invalidate` the same way |
| Thousands of live sites were marked dead | Every session fetched ten pages at startup, 30,000 connections in one second; local DNS and connection tracking dropped them, and the errors were read as "site is down" | Never fan out at startup. Let agents fetch as they go, through a pool. Tell the agent that a fetch error is not evidence, and give it a second, independent way to check |
| Reachable sites returned connection resets | The runtime's built-in HTTP client was being blocked by its network fingerprint | Fetch pages with `curl` and a browser user agent, or through a reader service, as a job tool |
| A reader API became the bottleneck | It allowed a fixed number of calls per minute | Read the limit from its response headers and set `perMinute` on the pool just under it. Tell the agent when the second check is worth using |
| Restarting lost everything in progress | Sessions and fetched pages lived only in memory | Built in: sessions are saved after every turn and restored on start; cache and limits persist. Yours: use `ctx.cache` in tools |
| Scheduler tables ended up in the business database | Convenience | Separate runtime database, always |
| The model sent `en: {}` before the real call | Parameter typed as "any" | Exact parameter schemas |
| Tool calls were cut off mid-argument | The answer exceeded the output limit | Let the agent save in parts |
| Writes to the cache failed on some pages | The page text contained NUL characters, which `jsonb` rejects | Built in: stripped before writing |
| "ETA 9 minutes" for an hour of work | Progress counted tasks, including ones with nothing to do | Provide `progress()` with your own done and total |
| A stuck run went unnoticed | Nobody was looking | Dashboard plus a scheduled check, below |
| The provider console showed no quota for the model | Some hosted models share capacity and have no fixed per-project limit | Look at past request counts by response code in the provider's monitoring to pick a starting limit, then let the gate adapt |

## Before a full run

1. Tools tested by hand against one item, including a bad input.
2. Destructive actions are soft and record a reason.
3. `gate.min`, `gate.limit` and each pool's caps are set from evidence, not guesses.
4. `progress()` returns your real done and total.
5. Run a small slice first: `pi-swarm run ./job.mjs --only-tasks 0001,0002,0003`. Read what was written. Check the share of destructive actions. A wrong rule found on thirty items is a note; found on thirty thousand it is a rollback.

## While it runs

```bash
pi-swarm status --job ./job.mjs
pi-swarm limit  --job ./job.mjs --pool gate --min 300 --limit 600
pi-swarm limit  --job ./job.mjs --pool reader --limit 300 --rpm 480
pi-swarm steer  --job ./job.mjs "New rule for all sessions: ..."
pi-swarm steer  --job ./job.mjs --interrupt "Stop the current turn and apply: ..."
pi-swarm stop   --job ./job.mjs
```

Changing limits and steering never need a restart. A restart is safe: sessions resume from their checkpoints and finished items are skipped.

The dashboard binds to `127.0.0.1` and is read-only. Put it behind your own proxy if others need to see it. Its top line is a verdict; the things to act on are queue wait approaching the request timeout, a rising share of 429 or aborted requests, and errors of one kind piling up.

### Scheduled check

For any run longer than about fifteen minutes, schedule a check every ten minutes (a cron job, or your agent harness's scheduler) that compares against its previous note and acts:

| Finding | Action |
|---|---|
| Process not running, or heartbeat older than two minutes, and work remains | Restart; it resumes |
| No new results since the last check | Restart |
| Recent upstream results are mostly non-429 errors (auth, bad request) | Stop and report. Restarting will not fix it and burns quota |
| Share of destructive actions jumped | Stop and report |
| A pool's queue keeps growing | Report; a person decides whether to raise the cap |
| All tasks done | Record it and disable the check |

Restart for stalls; stop for anything that looks like the job itself is wrong.

## After the run

Check counts in the business database against what the dashboard reported, sample the results by hand (especially the destructive ones), then export the business database. Drop the runtime database when you no longer need to resume.
