/**
 * Built-in bounded, PostgreSQL-cached web research tools (`webfetch` via curl + Chrome UA, `jina_read` via r.jina.ai).
 */
import fs from 'node:fs';
import { execFile } from 'node:child_process';

const DEFAULT_UA =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

const CURL_ERRORS = {
  6: 'DNS_RESOLVE_FAILED (curl 6)',
  7: 'CONNECT_FAILED (curl 7)',
  28: 'TIMEOUT (curl 28)',
  35: 'TLS_HANDSHAKE_FAILED (curl 35)',
  47: 'TOO_MANY_REDIRECTS (curl 47)',
  52: 'EMPTY_REPLY (curl 52)',
  56: 'CONNECTION_RESET (curl 56)',
  60: 'TLS_CERT_INVALID (curl 60)',
};

export function validPublicUrl(rawUrl) {
  try {
    const u = new URL(rawUrl);
    if (!['http:', 'https:'].includes(u.protocol)) return null;
    if (
      ['localhost', '127.0.0.1', '0.0.0.0', '::1', '169.254.169.254', 'metadata.google.internal'].includes(
        u.hostname
      )
    ) {
      return null;
    }
    return u.href;
  } catch {
    return null;
  }
}

function resolveJinaKey(jinaKeyOption) {
  if (!jinaKeyOption) {
    if (process.env.JINA_API_KEY) return process.env.JINA_API_KEY.trim();
    const defaultSecret = `${process.env.HOME || ''}/.config/pi-swarm/jina_api_key`;
    try {
      if (fs.existsSync(defaultSecret)) return fs.readFileSync(defaultSecret, 'utf8').trim();
    } catch {}
    return '';
  }
  if (jinaKeyOption.startsWith('$')) return (process.env[jinaKeyOption.slice(1)] || '').trim();
  if (jinaKeyOption.startsWith('/') || jinaKeyOption.startsWith('~')) {
    const p = jinaKeyOption.replace(/^~/, process.env.HOME || '');
    try {
      return fs.readFileSync(p, 'utf8').trim();
    } catch {
      return '';
    }
  }
  return jinaKeyOption.trim();
}

export function createBuiltinTools({
  Type,
  runtimeDb,
  withCurlSlot,
  withJinaSlot,
  nativeFetch = globalThis.fetch,
  send = () => {},
  cacheTtlSec = 86_400,
  jinaApiKey = null,
}) {
  const jinaKey = resolveJinaKey(jinaApiKey);

  async function curlFetch(rawUrl, maxChars = 8000) {
    const url = validPublicUrl(rawUrl);
    if (!url) return { ok: false, error: 'INVALID_OR_DISALLOWED_URL', url: rawUrl };

    if (runtimeDb && cacheTtlSec > 0) {
      const hit = await runtimeDb.getCachedFetch(url, 'curl', cacheTtlSec);
      if (hit) return hit;
    }

    const res = await withCurlSlot(
      () =>
        new Promise(resolve => {
          execFile(
            'curl',
            [
              '-sSL',
              '--compressed',
              '--max-time',
              '20',
              '--max-redirs',
              '8',
              '--max-filesize',
              '3000000',
              '-A',
              DEFAULT_UA,
              '-H',
              'Accept: text/html,application/xhtml+xml,*/*;q=0.8',
              '-H',
              'Accept-Language: en-US,en;q=0.9',
              '-w',
              '\n__CURL_META__ %{http_code} %{url_effective}',
              url,
            ],
            { maxBuffer: 8 * 1024 * 1024, timeout: 25_000, encoding: 'utf8' },
            (err, stdout = '', stderr = '') => {
              const metaIdx = stdout.lastIndexOf('\n__CURL_META__ ');
              const html = metaIdx >= 0 ? stdout.slice(0, metaIdx) : stdout;
              const [code, finalUrl] =
                metaIdx >= 0 ? stdout.slice(metaIdx + 15).trim().split(' ') : ['0', url];
              const httpStatus = Number(code) || 0;
              if (err && !httpStatus) {
                return resolve({
                  ok: false,
                  url,
                  httpStatus: 0,
                  error:
                    CURL_ERRORS[err.code] ||
                    `curl exit ${err.code ?? 'killed'}: ${String(stderr).trim().slice(0, 160)}`,
                });
              }
              const pick = re => (html.match(re)?.[1] || '').replace(/\s+/g, ' ').trim();
              const text = html
                .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
                .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
                .replace(/<[^>]+>/g, ' ')
                .replace(/&nbsp;/g, ' ')
                .replace(/\s+/g, ' ')
                .trim();
              resolve({
                ok: httpStatus >= 200 && httpStatus < 400,
                url,
                httpStatus,
                finalUrl,
                title: pick(/<title[^>]*>([\s\S]*?)<\/title>/i).slice(0, 200),
                metaDescription: (
                  pick(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["']/i) ||
                  pick(/<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']*)["']/i)
                ).slice(0, 400),
                bodyExcerpt: text.slice(0, maxChars),
              });
            }
          );
        })
    );

    if (runtimeDb && cacheTtlSec > 0) {
      await runtimeDb.putCachedFetch(url, 'curl', res.ok, res).catch(() => {});
    }
    return res;
  }

  async function jinaRead(rawUrl, maxChars = 10_000) {
    const url = validPublicUrl(rawUrl);
    if (!url) return { ok: false, error: 'INVALID_OR_DISALLOWED_URL', url: rawUrl };
    if (!jinaKey) return { ok: false, error: 'JINA_KEY_MISSING' };

    if (runtimeDb && cacheTtlSec > 0) {
      const hit = await runtimeDb.getCachedFetch(url, 'jina', cacheTtlSec);
      if (hit) return hit;
    }

    const res = await withJinaSlot(async () => {
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), 25_000);
      try {
        const r = await nativeFetch('https://r.jina.ai/' + url, {
          signal: ac.signal,
          headers: {
            authorization: `Bearer ${jinaKey}`,
            'x-timeout': '15',
            'x-retain-images': 'none',
            accept: 'text/plain',
          },
        });
        const body = await r.text();
        send({
          type: 'jina_usage',
          tokens: Number(r.headers.get('x-usage-tokens')) || 0,
          status: r.status,
        });
        return { ok: r.ok, url, jinaStatus: r.status, markdown: body.slice(0, maxChars) };
      } catch (err) {
        return {
          ok: false,
          url,
          error: String(err.cause?.code || err.message || 'JINA_ERROR').slice(0, 160),
        };
      } finally {
        clearTimeout(timer);
      }
    });

    if (runtimeDb && cacheTtlSec > 0 && res.jinaStatus !== 429) {
      await runtimeDb.putCachedFetch(url, 'jina', res.ok, res).catch(() => {});
    }
    return res;
  }

  const webfetch = {
    name: 'webfetch',
    label: 'Fetch a web page directly (curl, Chrome UA, PG-cached)',
    description:
      'Fetch a public URL with curl and a Chrome user agent. Results are cached in PostgreSQL across restarts. Network errors (reset, timeout, TLS, 403) often indicate crawler blocking rather than a dead site.',
    parameters: Type.Object({ url: Type.String() }),
    execute: async (_id, { url }) => ({
      content: [{ type: 'text', text: JSON.stringify(await curlFetch(url)) }],
    }),
  };

  const jinaReadTool = {
    name: 'jina_read',
    label: 'Read a web page via Jina Reader (PG-cached)',
    description:
      'Read a public URL through Jina Reader (r.jina.ai), returning clean rendered Markdown. Use when direct webfetch is blocked, fails with TLS/reset, or returns an empty SPA shell.',
    parameters: Type.Object({ url: Type.String() }),
    execute: async (_id, { url }) => ({
      content: [{ type: 'text', text: JSON.stringify(await jinaRead(url)) }],
    }),
  };

  return { webfetch, jinaRead: jinaReadTool, curlFetch, jinaReadRaw: jinaRead };
}
