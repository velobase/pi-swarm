/**
 * Public API and liberal input-normalization helpers for @velobase/pi-swarm.
 */

export function parseMaybeJson(val) {
  if (typeof val === 'string') {
    const trimmed = val.trim();
    if (
      (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
      (trimmed.startsWith('[') && trimmed.endsWith(']'))
    ) {
      try {
        return JSON.parse(trimmed);
      } catch {}
    }
  }
  return val;
}

export function toJsonArray(val) {
  const parsed = parseMaybeJson(val);
  return Array.isArray(parsed) ? parsed : [];
}

export function normalizeKeyedMap(val, { keyField = 'id', aliases = {} } = {}) {
  const parsed = parseMaybeJson(val);
  if (!parsed) return {};
  if (Array.isArray(parsed)) {
    const out = {};
    for (const raw of parsed) {
      const item = parseMaybeJson(raw);
      if (item && typeof item === 'object' && item[keyField]) {
        const k = aliases[item[keyField]] || item[keyField];
        out[k] = item;
      }
    }
    return out;
  }
  if (typeof parsed === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(parsed)) {
      const normKey = aliases[k] || k;
      out[normKey] = parseMaybeJson(v);
    }
    return out;
  }
  return {};
}

export function fuzzyResolveId(ownedIds, inputId) {
  if (!Array.isArray(ownedIds) || ownedIds.length === 0) return null;
  if (ownedIds.includes(inputId)) return inputId;
  const slug = s =>
    String(s || '')
      .toLowerCase()
      .replace(/[^a-z0-9\u4e00-\u9fa5]+/g, '-')
      .replace(/^-+|-+$/g, '');
  const alnum = s =>
    String(s || '')
      .toLowerCase()
      .replace(/[^a-z0-9\u4e00-\u9fa5]/g, '');
  const targetSlug = slug(inputId);
  const targetAlnum = alnum(inputId);
  if (!targetAlnum) return null;
  return (
    ownedIds.find(
      id =>
        id.toLowerCase() === String(inputId || '').toLowerCase() ||
        slug(id) === targetSlug ||
        alnum(id) === targetAlnum ||
        slug(id).startsWith(targetSlug) ||
        targetSlug.startsWith(slug(id)) ||
        alnum(id).startsWith(targetAlnum) ||
        targetAlnum.startsWith(alnum(id))
    ) || null
  );
}

export function defineSwarm(config) {
  if (!config || typeof config !== 'object') {
    throw new Error('defineSwarm requires a configuration object');
  }
  if (!config.runId || typeof config.runId !== 'string') {
    throw new Error('defineSwarm requires a non-empty string `runId`');
  }
  if (typeof config.tasks !== 'function' && !Array.isArray(config.tasks)) {
    throw new Error('defineSwarm requires `tasks` (array or async function returning [{ id, payload }])');
  }
  if (typeof config.buildPrompt !== 'function') {
    throw new Error('defineSwarm requires `buildPrompt(task, pending, ctx)`');
  }
  if (!config.model || typeof config.model !== 'object' || !config.model.id) {
    throw new Error('defineSwarm requires `model: { provider, id, ... }`');
  }

  return {
    concurrency: 3000,
    workers: 30,
    cwd: process.cwd(),
    retainSessions: false,
    httpIdleTimeoutMs: 30 * 60_000,
    maxContinuations: 5,
    retry: {
      enabled: true,
      maxRetries: 30,
      baseDelayMs: 1000,
      maxAgentDelayMs: 15_000,
    },
    gate: {
      limit: 300,
      min: 50,
      max: 1500,
      increaseStep: 0.1,
      decreaseFactor: 0.7,
      cooldownMinMs: 2000,
      cooldownJitterMs: 3000,
    },
    pools: {},
    ...config,
  };
}

export { Gate, SlotPool, createGcloudVertexAuth, createStaticAuth } from './gate.mjs';
export { RuntimeDb } from './runtime-db.mjs';
