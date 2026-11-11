/**
 * Global AIMD LLM concurrency + TPM gate, priority retry queue, pause/circuit-breaker support,
 * multi-endpoint / fallback model router, and bounded external tool SlotPool.
 */
import { execFile } from 'node:child_process';

const DEFAULT_BACKOFF_STATUSES = new Set([429, 503, 529]);
const BREAKER_FATAL_STATUSES = new Set([400, 401, 403, 404, 422]);

export class Gate {
  constructor({
    limit = 300,
    min = 50,
    max = 1500,
    tpm = 0,
    increaseStep = 0.1,
    decreaseFactor = 0.7,
    cooldownMinMs = 2000,
    cooldownJitterMs = 3000,
  } = {}) {
    Object.assign(this, {
      limit,
      min,
      max,
      tpm,
      increaseStep,
      decreaseFactor,
      cooldownMinMs,
      cooldownJitterMs,
    });
    this.held = new Map(); // grantId -> { workerIndex, taskId }
    this.highQueue = [];   // { id, workerIndex, taskId, estTokens, grant }
    this.normalQueue = [];
    this.cooldownUntil = 0;
    this.lastDecreaseAt = 0;
    this.paused = false;
    this.pauseReason = null;
    this.timer = null;
    this.tokenEvents = []; // { at, tokens }
    this.recentOutcomes = []; // { at, status }
    this.window = { ok: 0, backoff: 0, other: 0, net: 0 };
    this.total = { ok: 0, backoff: 0, other: 0, net: 0, granted: 0 };
  }

  get inflight() {
    return this.held.size;
  }

  get waitingCount() {
    return this.highQueue.length + this.normalQueue.length;
  }

  pause(reason = 'manual') {
    this.paused = true;
    this.pauseReason = String(reason);
  }

  resume() {
    this.paused = false;
    this.pauseReason = null;
    this.recentOutcomes.length = 0;
    this.pump();
  }

  pruneRolling(now = Date.now()) {
    while (this.tokenEvents.length && now - this.tokenEvents[0].at >= 60_000) {
      this.tokenEvents.shift();
    }
    while (this.recentOutcomes.length && now - this.recentOutcomes[0].at >= 60_000) {
      this.recentOutcomes.shift();
    }
  }

  tokensLastMinute(now = Date.now()) {
    this.pruneRolling(now);
    let sum = 0;
    for (const e of this.tokenEvents) sum += e.tokens;
    return sum;
  }

  recordTokens(tokens) {
    const n = Number(tokens) || 0;
    if (n > 0) this.tokenEvents.push({ at: Date.now(), tokens: n });
  }

  hasTaskActivity(taskId) {
    if (!taskId) return false;
    for (const v of this.held.values()) if (v.taskId === taskId) return true;
    for (const w of this.highQueue) if (w.taskId === taskId) return true;
    for (const w of this.normalQueue) if (w.taskId === taskId) return true;
    return false;
  }

  acquire(id, workerIndex, grant, { priority = 0, taskId = null, estTokens = 0 } = {}) {
    const entry = { id, workerIndex, taskId, estTokens: Math.max(0, Number(estTokens) || 0), grant };
    if (priority > 0) this.highQueue.push(entry);
    else this.normalQueue.push(entry);
    this.pump();
  }

  scheduleTimer(ms) {
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.pump();
    }, Math.max(10, ms));
  }

  pump() {
    if (this.paused) return;
    const now = Date.now();
    if (now < this.cooldownUntil) {
      this.scheduleTimer(this.cooldownUntil - now);
      return;
    }

    let usedTokens = this.tpm > 0 ? this.tokensLastMinute(now) : 0;

    while (this.held.size < Math.floor(this.limit) && (this.highQueue.length || this.normalQueue.length)) {
      const peek = this.highQueue.length ? this.highQueue[0] : this.normalQueue[0];
      if (this.tpm > 0 && usedTokens > 0 && usedTokens + peek.estTokens > this.tpm) {
        const oldest = this.tokenEvents[0];
        const waitMs = oldest ? 60_000 - (now - oldest.at) + 10 : 250;
        this.scheduleTimer(waitMs);
        return;
      }
      const next = this.highQueue.length ? this.highQueue.shift() : this.normalQueue.shift();
      this.held.set(next.id, { workerIndex: next.workerIndex, taskId: next.taskId });
      if (next.estTokens > 0) {
        this.tokenEvents.push({ at: now, tokens: next.estTokens });
        usedTokens += next.estTokens;
      }
      this.total.granted++;
      next.grant();
    }
  }

  release(id, status) {
    this.held.delete(id);
    const now = Date.now();
    this.recentOutcomes.push({ at: now, status });
    this.pruneRolling(now);

    if (DEFAULT_BACKOFF_STATUSES.has(status)) {
      this.window.backoff++;
      this.total.backoff++;
      if (now - this.lastDecreaseAt > this.cooldownMinMs) {
        this.limit = Math.max(this.min, this.limit * this.decreaseFactor);
        this.lastDecreaseAt = now;
        this.cooldownUntil = Math.max(
          this.cooldownUntil,
          now + this.cooldownMinMs + Math.random() * this.cooldownJitterMs
        );
      }
    } else if (status === 0) {
      this.window.net++;
      this.total.net++;
    } else if (status >= 200 && status < 400) {
      this.window.ok++;
      this.total.ok++;
      this.limit = Math.min(this.max, this.limit + this.increaseStep);
    } else {
      this.window.other++;
      this.total.other++;
    }
    this.pump();
  }

  /**
   * Checks whether non-retryable 4xx (e.g. 401/403/400) in the rolling 60s window
   * exceed the circuit-breaker threshold.
   */
  checkCircuitBreaker({ minRequests = 20, fatalShare = 0.5 } = {}) {
    const now = Date.now();
    this.pruneRolling(now);
    const total = this.recentOutcomes.length;
    if (total < minRequests) return null;
    let fatal = 0;
    const byStatus = {};
    for (const o of this.recentOutcomes) {
      if (BREAKER_FATAL_STATUSES.has(o.status)) {
        fatal++;
        byStatus[o.status] = (byStatus[o.status] || 0) + 1;
      }
    }
    const share = fatal / total;
    if (share >= fatalShare) {
      const topStatus = Object.entries(byStatus).sort((a, b) => b[1] - a[1])[0]?.[0] || '4xx';
      return {
        tripped: true,
        share,
        fatal,
        total,
        topStatus: Number(topStatus) || topStatus,
        reason: `circuit_breaker: ${fatal}/${total} requests failed with HTTP ${topStatus} in last 60s`,
      };
    }
    return null;
  }

  dropWorker(workerIndex) {
    this.highQueue = this.highQueue.filter(w => w.workerIndex !== workerIndex);
    this.normalQueue = this.normalQueue.filter(w => w.workerIndex !== workerIndex);
    for (const [id, v] of this.held) {
      if (v.workerIndex === workerIndex) this.held.delete(id);
    }
    this.pump();
  }

  setLimits({ limit, min, max, tpm } = {}) {
    if (Number.isFinite(min) && min > 0) this.min = min;
    if (Number.isFinite(max) && max >= this.min) this.max = max;
    if (Number.isFinite(limit) && limit > 0) {
      this.limit = Math.min(this.max, Math.max(this.min, limit));
    }
    if (Number.isFinite(tpm) && tpm >= 0) this.tpm = tpm;
    this.pump();
  }

  snapshot(resetWindow = false) {
    const now = Date.now();
    const s = {
      limit: Math.floor(this.limit),
      rawLimit: Number(this.limit.toFixed(2)),
      min: this.min,
      max: this.max,
      tpm: this.tpm,
      tokensLastMinute: this.tokensLastMinute(now),
      paused: this.paused,
      pauseReason: this.pauseReason,
      inflight: this.held.size,
      waiting: this.waitingCount,
      priorityWaiting: this.highQueue.length,
      coolingMs: Math.max(0, Math.round(this.cooldownUntil - now)),
      window: { ...this.window },
      total: { ...this.total },
    };
    if (resetWindow) this.window = { ok: 0, backoff: 0, other: 0, net: 0 };
    return s;
  }

  close() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }
}

export class SlotPool {
  constructor(limit = 120, perMinute = 0) {
    this.limit = limit;
    this.perMinute = perMinute;
    this.held = new Map();
    this.waiters = [];
    this.grantTimes = [];
    this.timer = null;
  }

  acquire(id, workerIndex, grant) {
    this.waiters.push({ id, workerIndex, grant });
    this.pump();
  }

  pump() {
    const now = Date.now();
    while (this.grantTimes.length && now - this.grantTimes[0] >= 60_000) {
      this.grantTimes.shift();
    }
    while (this.held.size < this.limit && this.waiters.length) {
      if (this.perMinute > 0 && this.grantTimes.length >= this.perMinute) {
        if (!this.timer) {
          const waitMs = Math.max(10, 60_000 - (now - this.grantTimes[0]) + 5);
          this.timer = setTimeout(() => {
            this.timer = null;
            this.pump();
          }, waitMs);
        }
        return;
      }
      const w = this.waiters.shift();
      this.held.set(w.id, w.workerIndex);
      this.grantTimes.push(now);
      w.grant();
    }
  }

  release(id) {
    if (this.held.delete(id)) this.pump();
  }

  dropWorker(workerIndex) {
    this.waiters = this.waiters.filter(w => w.workerIndex !== workerIndex);
    for (const [id, wi] of this.held) {
      if (wi === workerIndex) this.held.delete(id);
    }
    this.pump();
  }

  setLimits({ limit, perMinute } = {}) {
    if (Number.isFinite(limit) && limit > 0) this.limit = limit;
    if (Number.isFinite(perMinute) && perMinute >= 0) this.perMinute = perMinute;
    this.pump();
  }

  snapshot() {
    const now = Date.now();
    return {
      limit: this.limit,
      perMinute: this.perMinute,
      inflight: this.held.size,
      waiting: this.waiters.length,
      lastMinute: this.grantTimes.filter(t => now - t < 60_000).length,
    };
  }

  close() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }
}

export function createStaticAuth(tokenOrEnvName) {
  return {
    async getToken() {
      if (!tokenOrEnvName) return null;
      if (tokenOrEnvName.startsWith('$')) {
        return process.env[tokenOrEnvName.slice(1)] || null;
      }
      return tokenOrEnvName;
    },
    invalidate() {},
  };
}

export function createGcloudVertexAuth({ nativeFetch = globalThis.fetch } = {}) {
  let cache = { value: null, expiresAt: 0, pending: null };
  return {
    invalidate(bad) {
      if (!bad || cache.value === bad) {
        cache.value = null;
        cache.expiresAt = 0;
      }
    },
    getToken() {
      if (cache.value && Date.now() < cache.expiresAt) {
        return Promise.resolve(cache.value);
      }
      cache.pending ??= (async () => {
        try {
          const value = await new Promise((resolve, reject) =>
            execFile('gcloud', ['auth', 'print-access-token'], { timeout: 30_000 }, (err, out) =>
              err ? reject(err) : resolve(out.trim())
            )
          );
          let ttlSec = 600;
          try {
            const infoRes = await nativeFetch(
              'https://oauth2.googleapis.com/tokeninfo?access_token=' + encodeURIComponent(value)
            );
            const info = await infoRes.json();
            if (Number(info.expires_in) > 0) ttlSec = Number(info.expires_in);
          } catch {}
          cache.value = value;
          cache.expiresAt = Date.now() + Math.max(30, ttlSec - 120) * 1000;
          return value;
        } finally {
          cache.pending = null;
        }
      })();
      return cache.pending;
    },
  };
}

export function resolveAuthSpec(spec, { nativeFetch = globalThis.fetch } = {}) {
  if (!spec) return null;
  if (spec === 'gcloud-vertex') return createGcloudVertexAuth({ nativeFetch });
  if (typeof spec === 'object' && typeof spec.getToken === 'function') return spec;
  if (typeof spec === 'string') return createStaticAuth(spec);
  return null;
}

/**
 * Round-robin + cooldown router across multiple endpoints / API keys + optional fallback model.
 * Each endpoint: { name?, baseUrl, model?, auth?|apiKey?, fallback?: boolean, cooldownMs?: number }
 */
export class EndpointRouter {
  constructor(endpoints = [], { nativeFetch = globalThis.fetch } = {}) {
    this.rr = 0;
    this.endpoints = endpoints.map((ep, idx) => ({
      name: ep.name || (ep.fallback ? `fallback-${idx}` : `ep-${idx}`),
      baseUrl: String(ep.baseUrl || '').replace(/\/+$/, ''),
      model: ep.model || null,
      fallback: Boolean(ep.fallback),
      cooldownMs: Number(ep.cooldownMs || 10_000),
      auth: resolveAuthSpec(ep.auth || ep.apiKey, { nativeFetch }),
      cooldownUntil: 0,
      stats: { ok: 0, backoff: 0, other: 0 },
    }));
  }

  pick() {
    if (this.endpoints.length === 0) return null;
    const now = Date.now();
    const primaries = this.endpoints.filter(e => !e.fallback);
    const readyPrimaries = primaries.filter(e => now >= e.cooldownUntil);
    if (readyPrimaries.length > 0) {
      return readyPrimaries[this.rr++ % readyPrimaries.length];
    }
    const fallbacks = this.endpoints.filter(e => e.fallback);
    const readyFallbacks = fallbacks.filter(e => now >= e.cooldownUntil);
    if (readyFallbacks.length > 0) {
      return readyFallbacks[this.rr++ % readyFallbacks.length];
    }
    // All cooling down: pick whichever wakes up earliest.
    return [...this.endpoints].sort((a, b) => a.cooldownUntil - b.cooldownUntil)[0];
  }

  report(ep, status) {
    if (!ep) return;
    if (DEFAULT_BACKOFF_STATUSES.has(status) || status >= 500) {
      ep.stats.backoff++;
      ep.cooldownUntil = Date.now() + ep.cooldownMs;
    } else if (status >= 200 && status < 400) {
      ep.stats.ok++;
    } else if (status > 0) {
      ep.stats.other++;
    }
  }

  snapshot() {
    const now = Date.now();
    return this.endpoints.map(e => ({
      name: e.name,
      fallback: e.fallback,
      model: e.model,
      coolingMs: Math.max(0, e.cooldownUntil - now),
      ...e.stats,
    }));
  }
}

function estimateBodyTokens(body) {
  if (!body) return 0;
  if (typeof body === 'string') return Math.ceil(Buffer.byteLength(body, 'utf8') / 3.5);
  if (body instanceof Uint8Array || Buffer.isBuffer(body)) return Math.ceil(body.byteLength / 3.5);
  return 0;
}

function rewriteEndpointRequest(url, init, primaryBaseUrl, ep, thoughtSigCache) {
  let targetUrl = url;
  if (ep && primaryBaseUrl && ep.baseUrl && url.startsWith(primaryBaseUrl)) {
    targetUrl = ep.baseUrl + url.slice(primaryBaseUrl.length);
  }
  let targetInit = init;
  if (typeof init?.body === 'string') {
    try {
      const parsed = JSON.parse(init.body);
      let mutated = false;
      if (ep?.model && parsed && typeof parsed === 'object' && parsed.model) {
        parsed.model = ep.model;
        mutated = true;
      }
      if (thoughtSigCache && Array.isArray(parsed?.messages)) {
        for (const msg of parsed.messages) {
          if (msg?.role === 'assistant' && Array.isArray(msg.tool_calls)) {
            for (const tc of msg.tool_calls) {
              const sig = tc?.id && thoughtSigCache.get(tc.id);
              if (sig && !tc.extra_content?.google?.thought_signature) {
                tc.extra_content = {
                  ...(tc.extra_content || {}),
                  google: { ...(tc.extra_content?.google || {}), thought_signature: sig },
                };
                mutated = true;
              }
            }
          }
        }
      }
      if (mutated) {
        targetInit = { ...init, body: JSON.stringify(parsed) };
      }
    } catch {}
  }
  return { targetUrl, targetInit };
}

export function installGatedFetch({
  send,
  matchUrl,
  primaryBaseUrl = null,
  auth = null,
  router = null,
  getTaskContext = () => null,
  onEvent = () => {},
}) {
  const nativeFetch = globalThis.fetch;
  const grants = new Map();
  const retryTracker = new Map();
  const thoughtSigCache = new Map(); // tool_call.id -> thought_signature (bounded LRU)
  let seq = 0;

  function harvestThoughtSignaturesFromText(text) {
    if (!text || !text.includes('thought_signature')) return;
    for (const line of text.split('\n')) {
      if (!line.includes('thought_signature')) continue;
      const jsonStr = line.startsWith('data:') ? line.slice(5).trim() : line.trim();
      try {
        const obj = JSON.parse(jsonStr);
        for (const ch of obj.choices || []) {
          const tcs = ch.delta?.tool_calls || ch.message?.tool_calls || [];
          for (const tc of tcs) {
            const id = tc?.id;
            const sig = tc?.extra_content?.google?.thought_signature;
            if (id && sig) {
              thoughtSigCache.set(id, sig);
              if (thoughtSigCache.size > 50_000) {
                const oldest = thoughtSigCache.keys().next().value;
                thoughtSigCache.delete(oldest);
              }
            }
          }
        }
      } catch {}
    }
  }

  process.on('message', m => {
    if (m?.type === 'llm_grant') {
      grants.get(m.id)?.();
      grants.delete(m.id);
    }
  });

  const matches = url => {
    if (!matchUrl) return false;
    if (typeof matchUrl === 'function') return matchUrl(url);
    if (typeof matchUrl === 'string') return url.includes(matchUrl);
    if (matchUrl instanceof RegExp) return matchUrl.test(url);
    return false;
  };

  const cleanPrimaryBase = primaryBaseUrl ? String(primaryBaseUrl).replace(/\/+$/, '') : null;

  globalThis.fetch = async (input, init = {}) => {
    const origUrl = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    if (!matches(origUrl)) return nativeFetch(input, init);

    const taskId = getTaskContext();
    const priorFails = taskId ? retryTracker.get(taskId) || 0 : 0;
    const priority = priorFails > 0 ? 1 : 0;
    const estTokens = estimateBodyTokens(init?.body);

    const slotId = `llm-${process.pid}-${++seq}`;
    const queuedAt = Date.now();
    await new Promise(resolve => {
      grants.set(slotId, resolve);
      send({ type: 'llm_acquire', id: slotId, priority, taskId, estTokens });
    });

    const ep = router ? router.pick() : null;
    const activeAuth = ep?.auth || auth;
    const { targetUrl, targetInit } = rewriteEndpointRequest(
      origUrl,
      init,
      cleanPrimaryBase,
      ep,
      thoughtSigCache
    );

    const startedAt = Date.now();
    let released = false;
    let res;
    const release = status => {
      if (released) return;
      released = true;
      if (ep) router.report(ep, status);
      if (taskId) {
        if (status >= 200 && status < 400) retryTracker.delete(taskId);
        else retryTracker.set(taskId, (retryTracker.get(taskId) || 0) + 1);
      }
      send({
        type: 'llm_release',
        id: slotId,
        status,
        endpoint: ep?.name || null,
        waitMs: startedAt - queuedAt,
        durMs: Date.now() - startedAt,
      });
    };

    targetInit.signal?.addEventListener('abort', () => release(res ? res.status : 0), { once: true });
    const safety = setTimeout(() => release(0), 35 * 60_000);
    safety.unref();

    try {
      const headers = new Headers(
        targetInit.headers ?? (input instanceof Request ? input.headers : undefined)
      );
      let token = activeAuth ? await activeAuth.getToken() : null;
      if (token) headers.set('authorization', `Bearer ${token}`);
      res = await nativeFetch(targetUrl, { ...targetInit, headers });

      if (res.status === 401 && activeAuth?.invalidate) {
        await res.body?.cancel().catch(() => {});
        activeAuth.invalidate(token);
        token = await activeAuth.getToken();
        if (token) headers.set('authorization', `Bearer ${token}`);
        res = await nativeFetch(targetUrl, { ...targetInit, headers });
      }
    } catch (err) {
      clearTimeout(safety);
      release(0);
      throw err;
    }

    onEvent({ type: 'llm_status', status: res.status, endpoint: ep?.name || null });
    if (!res.ok || !res.body) {
      clearTimeout(safety);
      release(res.status);
      return res;
    }
    const done = () => {
      clearTimeout(safety);
      release(res.status);
    };
    const dec = new TextDecoder();
    let lineBuf = '';
    const body = res.body.pipeThrough(
      new TransformStream({
        transform(chunk, controller) {
          try {
            lineBuf += dec.decode(chunk, { stream: true });
            const lastNl = lineBuf.lastIndexOf('\n');
            if (lastNl >= 0) {
              const completeLines = lineBuf.slice(0, lastNl);
              lineBuf = lineBuf.slice(lastNl + 1);
              if (completeLines.includes('thought_signature')) {
                harvestThoughtSignaturesFromText(completeLines);
              }
            }
          } catch {}
          controller.enqueue(chunk);
        },
        flush() {
          if (lineBuf.includes('thought_signature')) harvestThoughtSignaturesFromText(lineBuf);
          done();
        },
        cancel: done,
      })
    );
    return new Response(body, {
      status: res.status,
      statusText: res.statusText,
      headers: res.headers,
    });
  };

  return nativeFetch;
}

export function makeSlotClient(pool, send) {
  const grants = new Map();
  let seq = 0;
  process.on('message', m => {
    if (m?.type === 'slot_grant' && m.pool === pool) {
      grants.get(m.id)?.();
      grants.delete(m.id);
    }
  });
  return async fn => {
    const id = `${pool}-${process.pid}-${++seq}`;
    await new Promise(resolve => {
      grants.set(id, resolve);
      send({ type: 'slot_acquire', pool, id });
    });
    try {
      return await fn();
    } finally {
      send({ type: 'slot_release', pool, id });
    }
  };
}
