import pg from 'pg';
import { defineSwarm, parseMaybeJson, fuzzyResolveId } from '../src/index.mjs';

let businessPool;
const getPool = () =>
  (businessPool ??= new pg.Pool({
    connectionString: process.env.BUSINESS_DATABASE_URL,
    max: 20,
  }));

export default defineSwarm({
  runId: 'example-web-audit-v1',
  name: 'Example 3,000-Agent Web Audit & Copy Rewrite',
  concurrency: 3000,
  workers: 30,
  model: {
    provider: 'vertex-ds',
    id: 'deepseek-ai/deepseek-v4.1-flash-maas',
    vertexProject: process.env.GOOGLE_CLOUD_PROJECT || 'my-gcp-project',
    vertexLocation: 'global',
    auth: 'gcloud-vertex',
  },
  gate: { limit: 300, min: 50, max: 1500 },
  pools: {
    curl: { limit: 300 },
    jina: { limit: 300, perMinute: 480 },
  },

  async tasks() {
    const { rows } = await getPool().query(
      `SELECT id, url FROM items WHERE audit_status = 'pending' ORDER BY id ASC`
    );
    const out = [];
    for (let i = 0; i < rows.length; i += 10) {
      out.push({
        id: String(out.length + 1).padStart(4, '0'),
        payload: { items: rows.slice(i, i + 10) },
      });
    }
    return out;
  },

  async loadPending(task) {
    const ids = task.payload.items.map(x => x.id);
    const { rows } = await getPool().query(
      `SELECT id, url, title FROM items WHERE id = ANY($1::text[]) AND audit_status = 'pending'`,
      [ids]
    );
    return rows;
  },

  systemPrompt:
    'You are an autonomous web researcher. Inspect each assigned URL with webfetch (cross-check with jina_read if blocked), then persist structured output via save_result. Never ask questions.',

  buildPrompt: (_task, pending) =>
    `Process each pending item below and persist via save_result:\n${JSON.stringify(pending)}`,

  tools: (task, { Type, builtins }) => {
    const ownedIds = task.payload.items.map(x => x.id);
    return [
      builtins.webfetch,
      builtins.jinaRead,
      {
        name: 'save_result',
        label: 'Save audited item into PostgreSQL',
        description: 'Persist structured audit output for one assigned item.',
        parameters: Type.Object({
          id: Type.String(),
          status: Type.String(),
          summary: Type.String(),
        }),
        execute: async (_callId, raw) => {
          const args = parseMaybeJson(raw);
          const resolvedId = fuzzyResolveId(ownedIds, args.id);
          if (!resolvedId) {
            throw new Error(`Invalid id "${args.id}". Valid: ${ownedIds.join(', ')}`);
          }
          await getPool().query(
            `UPDATE items SET audit_status = $2, summary = $3, updated_at = now() WHERE id = $1`,
            [resolvedId, args.status, args.summary]
          );
          return { content: [{ type: 'text', text: JSON.stringify({ ok: true, id: resolvedId }) }] };
        },
      },
    ];
  },

  teardownWorker: async () => {
    await businessPool?.end();
  },
});
