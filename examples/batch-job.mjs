import pg from 'pg';
import { defineSwarm, parseMaybeJson, fuzzyResolveId } from '../src/index.mjs';

let businessPool;
const getPool = () =>
  (businessPool ??= new pg.Pool({
    connectionString: process.env.BUSINESS_DATABASE_URL,
    max: 20,
  }));

export default defineSwarm({
  runId: 'example-batch-v1',
  name: 'Example 3,000-Agent Autonomous Batch Job',
  concurrency: 3000,
  workers: 30,
  model: {
    provider: 'openai-compat',
    id: process.env.MODEL_ID || 'deepseek-chat',
    baseUrl: process.env.MODEL_BASE_URL || 'https://api.deepseek.com/v1',
    apiKey: '$MODEL_API_KEY',
  },
  // Declare any named external resource pools your tools need (optional):
  pools: {
    externalApi: { limit: 200, perMinute: 600 },
  },

  async tasks() {
    const { rows } = await getPool().query(
      `SELECT id FROM work_items WHERE status = 'pending' ORDER BY id ASC`
    );
    const out = [];
    for (let i = 0; i < rows.length; i += 10) {
      out.push({
        id: String(out.length + 1).padStart(4, '0'),
        payload: { ids: rows.slice(i, i + 10).map(r => r.id) },
      });
    }
    return out;
  },

  async loadPending(task) {
    const { rows } = await getPool().query(
      `SELECT id, input_data FROM work_items WHERE id = ANY($1::text[]) AND status = 'pending'`,
      [task.payload.ids]
    );
    return rows;
  },

  systemPrompt:
    'You are an autonomous worker agent. Inspect each assigned item, call fetch_context if needed, and save output via save_result. Never ask questions.',

  buildPrompt: (_task, pending) =>
    `Process each pending item below and persist via save_result:\n${JSON.stringify(pending)}`,

  // Tools are defined purely per-job, using generic ctx.pool / ctx.cache / ctx.metric primitives:
  tools: (task, { Type, pool, cache, metric, nativeFetch }) => {
    const ownedIds = task.payload.ids;
    return [
      {
        name: 'fetch_context',
        label: 'Fetch external context (pooled & PG-cached)',
        description: 'Call external API with automatic global rate-pooling and PostgreSQL caching.',
        parameters: Type.Object({ key: Type.String() }),
        execute: async (_callId, { key }) => {
          const hit = await cache.get('ext-api', key, 86_400);
          if (hit !== undefined) return { content: [{ type: 'text', text: JSON.stringify(hit) }] };
          const data = await pool('externalApi', async () => {
            const r = await nativeFetch(`https://example.com/api/${encodeURIComponent(key)}`);
            return { status: r.status, body: await r.text() };
          });
          metric('external_api_calls', 1);
          await cache.set('ext-api', key, data);
          return { content: [{ type: 'text', text: JSON.stringify(data) }] };
        },
      },
      {
        name: 'save_result',
        label: 'Save processed item into business PostgreSQL',
        description: 'Persist structured output for one assigned item.',
        parameters: Type.Object({
          id: Type.String(),
          output: Type.Any(),
        }),
        execute: async (_callId, raw) => {
          const args = parseMaybeJson(raw);
          const resolvedId = fuzzyResolveId(ownedIds, args.id);
          if (!resolvedId) {
            throw new Error(`Invalid id "${args.id}". Valid: ${ownedIds.join(', ')}`);
          }
          await getPool().query(
            `UPDATE work_items SET status = 'done', output_data = $2::jsonb, updated_at = now() WHERE id = $1`,
            [resolvedId, JSON.stringify(parseMaybeJson(args.output))]
          );
          metric('items_saved', 1);
          return { content: [{ type: 'text', text: JSON.stringify({ ok: true, id: resolvedId }) }] };
        },
      },
    ];
  },

  teardownWorker: async () => {
    await businessPool?.end();
  },
});
