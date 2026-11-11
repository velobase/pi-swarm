/**
 * Global AIMD LLM concurrency gate (with priority retry queue & worker leak protection),
 * bounded external tool SlotPool (concurrency + sliding-window RPM),
 * and transparent token refresh on HTTP 401.
 */
import { execFile } from 'node:child_process';

const DEFAULT_BACKOFF_STATUSES = new Set([429, 503, 529]);

export class Gate {
  constructor({
    limit = 300,
    min = 50,
    max = 1500,
    increaseStep = 0.1,
    decreaseFactor = 0.7,
    cooldownMinMs = 2000,
    cooldownJitterMs = 3000,
  } = {}) {
    Object.assign(this, {
      limit,
      min,
      max,
      increaseStep,
      decreaseFactor,
      cooldownMinMs,
      cooldownJitterMs,
    });
    this.held = new Map(); // grantId -> workerIndex
    this.highQueue = [];   // retried / priority requests (jump ahead of cold requests)
    this.normalQueue = []; // first-attempt requests
    this.cooldownUntil = 0;
    this.lastDecreaseAt = 0;
    this.timer = null;
    this.window = { ok: 0, backoff: 0, other: 0, net: 0 };
    this.total = { ok: 0, backoff: 0, other: 0, net: 0, granted: 0 };
  }

  get inflight() {
    return this.held.size;
  }

  get waitingCount() {
    return this.highQueue.length + this.normalQueue.length;
  }

  acquire(id, workerIndex, grant, { priority = 0 } = {}) {
    const entry = { id, workerIndex, grant };
    if (priority > 0) this.highQueue.push(entry);
    else this.normalQueue.push(entry);
    this.pump();
  }

  pump() {
    const now = Date.now();
    if (now < this.cooldownUntil) {
      if (!this.timer) {
        this.timer = setTimeout(() => {
          this.timer = null;
          this.pump();
        }, Math.max(5, this.cooldownUntil - now));
      }
      return;
    }
    while (this.held.size < Math.floor(this.limit) && (this.highQueue.length || this.normalQueue.length)) {
      const next = this.highQueue.length ? this.highQueue.shift() : this.normalQueue.shift();
      this.held.set(next.id, next.workerIndex);
      this.total.granted++;
      next.grant();
    }
  }

  release(id, status) {
    this.held.delete(id);
    const now = Date.now();
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

  dropWorker(workerIndex) {
    this.highQueue = this.highQueue.filter(w => w.workerIndex !== workerIndex);
    this.normalQueue = this.normalQueue.filter(w => w.workerIndex !== workerIndex);
    for (const [id, wi] of this.held) {
      if (wi === workerIndex) this.held.delete(id);
    }
    this.pump();
  }

  setLimits({ limit, min, max } = {}) {
    if (Number.isFinite(min) && min > 0) this.min = min;
    if (Number.isFinite(max) && max >= this.min) this.max = max;
    if (Number.isFinite(limit) && limit > 0) {
      this.limit = Math.min(this.max, Math.max(this.min, limit));
    }
    this.pump();
  }

  snapshot(resetWindow = false) {
    const s = {
      limit: Math.floor(this.limit),
      rawLimit: Number(this.limit.toFixed(2)),
      min: this.min,
      max: this.max,
      inflight: this.held.size,
      waiting: this.waitingCount,
      priorityWaiting: this.highQueue.length,
      coolingMs: Math.max(0, Math.round(this.cooldownUntil - Date.now())),
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

export function installGatedFetch({
  send,
  matchUrl,
  auth = null,
  getTaskContext = () => null,
  onEvent = () => {},
}) {
  const nativeFetch = globalThis.fetch;
  const grants = new Map();
  const retryTracker = new Map(); // taskId -> consecutive failures before 200
  let seq = 0;

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

  globalThis.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    if (!matches(url)) return nativeFetch(input, init);

    const taskId = getTaskContext();
    const priorFails = taskId ? retryTracker.get(taskId) || 0 : 0;
    const priority = priorFails > 0 ? 1 : 0;

    const slotId = `llm-${process.pid}-${++seq}`;
    const queuedAt = Date.now();
    await new Promise(resolve => {
      grants.set(slotId, resolve);
      send({ type: 'llm_acquire', id: slotId, priority });
    });

    const startedAt = Date.now();
    let released = false;
    let res;
    const release = status => {
      if (released) return;
      released = true;
      if (taskId) {
        if (status >= 200 && status < 400) retryTracker.delete(taskId);
        else retryTracker.set(taskId, (retryTracker.get(taskId) || 0) + 1);
      }
      send({
        type: 'llm_release',
        id: slotId,
        status,
        waitMs: startedAt - queuedAt,
        durMs: Date.now() - startedAt,
      });
    };

    init.signal?.addEventListener('abort', () => release(res ? res.status : 0), { once: true });
    const safety = setTimeout(() => release(0), 35 * 60_000);
    safety.unref();

    try {
      const headers = new Headers(
        init.headers ?? (input instanceof Request ? input.headers : undefined)
      );
      let token = auth ? await auth.getToken() : null;
      if (token) headers.set('authorization', `Bearer ${token}`);
      res = await nativeFetch(input, { ...init, headers });

      if (res.status === 401 && auth?.invalidate) {
        await res.body?.cancel().catch(() => {});
        auth.invalidate(token);
        token = await auth.getToken();
        if (token) headers.set('authorization', `Bearer ${token}`);
        res = await nativeFetch(input, { ...init, headers });
      }
    } catch (err) {
      clearTimeout(safety);
      release(0);
      throw err;
    }

    onEvent({ type: 'llm_status', status: res.status });
    if (!res.ok || !res.body) {
      clearTimeout(safety);
      release(res.status);
      return res;
    }
    const done = () => {
      clearTimeout(safety);
      release(res.status);
    };
    const body = res.body.pipeThrough(new TransformStream({ flush: done, cancel: done }));
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
