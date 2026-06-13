/**
 * worker-outbound — RIGHT of AI Gateway
 *
 * POST (Responses API envelope) → HTTP request to real target
 * HTTP ReadableStream → SSE back through AI Gateway
 *
 * SSE event schema:
 *   init  {type, status, headers, bytes, model, request_id, target_url}
 *   text  {type, chunk: string}
 *   bin   {type, chunk: base64}   — bytes payloads, per-chunk encoded
 *   done  {type, ms: number}
 *   error {type, message: string}
 *
 * error always emits a synthetic init first so inbound never hangs on initP.
 */

const newResponse = (...args) =>{
  try{
    return new Response(...args);
  }catch(e){
    if(e?.message === 'Response with null body status (101, 204, 205, or 304) cannot have a body.'){
      return new Response(null,...args.slice(1));
    }
    return new Response(String(e),{status:500,statusText:String(e)});
  }
};

const IS_TEXT = /text|html|script|xml|json|pdf/i;

const HOP_BY_HOP = new Set([
  'connection','keep-alive','proxy-authenticate','proxy-authorization',
  'te','trailers','transfer-encoding','upgrade',
  'host','cf-connecting-ip','cf-ray','cf-visitor','cf-ipcountry',
]);

export default {
  async fetch(request, env, ctx) {
    if(request.headers.get('authorization') !== `Bearer ${env.CF_AIG_AUTHORIZATION}`){
      return sseError(null,'AI Gateway unauthorized');
    }
    let payload;
    try { payload = await request.json(); }
    catch { return sseError(null, 'Invalid JSON body'); }

    const { model, input, metadata } = payload;
    if (!model) return sseError(null, 'Missing model field');

    const targetUrl = resolveTarget(model, metadata, env);
    if (!targetUrl) return sseError(model, `No route for model: ${model}`);

    const fwdHeaders = new Headers();
    for (const [k, v] of Object.entries(metadata?.headers ?? {})) {
      if (!HOP_BY_HOP.has(k.toLowerCase())) fwdHeaders.set(k, v);
    }

    if(fwdHeaders.get('outbound-api-key') !== env.OUTBOUND_API_KEY){
      return sseError(null, 'Missing outbound api key');
    }

    const method = (metadata?.method ?? 'GET').toUpperCase();
    let fwdBody;
    if (method !== 'GET' && method !== 'HEAD' && input != null) {
      fwdBody = metadata?.is_base64 ? base64ToU8(input) : input;
    }

    const start = Date.now();
    let targetRes;
    try {
      targetRes = await fetch(targetUrl, {
        method, headers: fwdHeaders, body: fwdBody, redirect: 'follow',
      });
    } catch (err) {
      return sseError(model, `Target unreachable: ${err.message}`);
    }

    const ct = targetRes.headers.get('content-type') ?? '';
    const isBytes = !IS_TEXT.test(ct);

    const enc = new TextEncoder();
    const sseEncode = (obj) => enc.encode(`data: ${JSON.stringify(obj)}\n\n`);

    let streamController;
    const readable = new ReadableStream({
      start(c) { streamController = c; },
    });

    const pipePromise = (async () => {
      try {
        streamController.enqueue(sseEncode({
          type: 'init',
          status: [101, 204, 205, 304].includes(targetRes.status) ? 200 : targetRes?.status ?? 200,
          headers: Object.fromEntries(targetRes.headers.entries()),
          bytes: isBytes,
          model,
          request_id: metadata?.request_id ?? null,
          target_url: targetUrl,
        }));

        if (targetRes.body) {
          const reader = targetRes.body.getReader();
          const dec = new TextDecoder();
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            streamController.enqueue(sseEncode(isBytes
              ? { type: 'bin',  chunk: u8ToBase64(value) }
              : { type: 'text', chunk: dec.decode(value, { stream: true }) }
            ));
          }
        }

        streamController.enqueue(sseEncode({ type: 'done', ms: Date.now() - start }));
      } catch (err) {
        try { streamController.enqueue(sseEncode({ type: 'error', message: err.message })); } catch {}
      } finally {
        try { streamController.close(); } catch {}
      }
    })();

    ctx.waitUntil(pipePromise);

    return newResponse(readable, {
      status: targetRes?.status ?? 200,
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
      },
    });
  },
};

// ── routing ────────────────────────────────────────────────────────────────────

function resolveTarget(model, metadata, env) {
  if (env.ROUTES) {
    let table;
    try { table = JSON.parse(env.ROUTES); } catch { /* bad JSON */ }
    if (table) {
      if (table[model]) return buildUrl(table[model], metadata);
      let best = null, bestLen = -1;
      for (const [prefix, url] of Object.entries(table)) {
        if (model.startsWith(prefix + '.') && prefix.length > bestLen) {
          best = url; bestLen = prefix.length;
        }
      }
      if (best) return buildUrl(best, metadata);
    }
  }
  if (env.TARGET_BASE_URL) return buildUrl(env.TARGET_BASE_URL, metadata);

  // Zero-config passthrough: first path segment that contains a dot → hostname.
  // /en.wikipedia.org/w/api.php → https://en.wikipedia.org/w/api.php
  // Bypasses buildUrl intentionally — avoids the double-hostname bug that
  // occurs when metadata.path still carries the host segment.
  const segs = (metadata?.path ?? '').replace(/^\//, '').split('/');
  if (segs[0]?.includes('.')) {
    const rest = segs.length > 1 ? '/' + segs.slice(1).join('/') : '/';
    return `https://${segs[0]}${rest}${metadata?.search ?? ''}`;
  }

  return null;
}

function buildUrl(base, metadata) {
  return base.replace(/\/+$/, '') + (metadata?.path ?? '') + (metadata?.search ?? '');
}

// ── SSE helpers ────────────────────────────────────────────────────────────────

/** Always emits init first so worker-inbound's initP always resolves. */
function sseError(model, message) {
  const lines = [
    `data: ${JSON.stringify({ type: 'init', status: 502, headers: {}, bytes: false, model: model ?? 'unknown', request_id: null })}\n\n`,
    `data: ${JSON.stringify({ type: 'text', chunk: message })}\n\n`,
    `data: ${JSON.stringify({ type: 'done', ms: 0 })}\n\n`,
  ].join('');
  return newResponse(lines, {
    status: 500,
    headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
  });
}

// ── codec ──────────────────────────────────────────────────────────────────────

function u8ToBase64(u8) {
  let s = '';
  const len = u8.length;
  for (let i = 0; i !== len; ++i) s += String.fromCharCode(u8[i]);
  return btoa(s);
}

function base64ToU8(b64) {
  const bin = atob(b64);
  const u8  = new Uint8Array(bin.length);
  const len = bin.length;
  for (let i = 0; i !== len; ++i) u8[i] = bin.charCodeAt(i);
  return u8;
}
