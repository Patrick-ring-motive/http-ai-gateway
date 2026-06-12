/**
 * worker-outbound-oai — RIGHT of AI Gateway (OpenAI Chat Completions format)
 *
 * POST (Chat Completions envelope) → HTTP request to real target
 * HTTP ReadableStream → SSE (Chat Completions chunks) back through AI Gateway
 *
 * The metadata (method, headers, etc.) arrives in messages[0] (system role).
 * The request body arrives in messages[1] (user role).
 *
 * Response SSE uses Chat Completions chunk format with our internal protocol
 * (init/text/bin/done/error) JSON-encoded inside choices[0].delta.content.
 * Stream ends with data: [DONE].
 */

const newResponse = (...args) => {
  try {
    return new Response(...args);
  } catch (e) {
    if (e?.message === 'Response with null body status (101, 204, 205, or 304) cannot have a body.') {
      return new Response(null, ...args.slice(1));
    }
    return new Response(String(e), { status: 500, statusText: String(e) });
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
    if (request.headers.get('authorization') !== `Bearer ${env.CF_AIG_AUTHORIZATION}`) {
      return chatError(null, 'AI Gateway unauthorized');
    }

    let payload;
    try { payload = await request.json(); }
    catch { return chatError(null, 'Invalid JSON body'); }

    const { model, messages } = payload;
    if (!model) return chatError(null, 'Missing model field');
    if (!Array.isArray(messages) || messages.length === 0) {
      return chatError(model, 'Missing messages array');
    }

    // Extract metadata from system message, input from user message
    let metadata;
    try { metadata = JSON.parse(messages[0]?.content ?? '{}'); }
    catch { metadata = {}; }
    const input = messages.find(m => m.role === 'user')?.content ?? null;

    const targetUrl = resolveTarget(model, metadata, env);
    if (!targetUrl) return chatError(model, `No route for model: ${model}`);

    const fwdHeaders = new Headers();
    for (const [k, v] of Object.entries(metadata?.headers ?? {})) {
      if (!HOP_BY_HOP.has(k.toLowerCase())) fwdHeaders.set(k, v);
    }

    if (fwdHeaders.get('outbound-api-key') !== env.OUTBOUND_API_KEY) {
      return chatError(null, 'Missing outbound api key');
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
      return chatError(model, `Target unreachable: ${err.message}`);
    }

    const ct = targetRes.headers.get('content-type') ?? '';
    const isBinary = !IS_TEXT.test(ct);

    const requestId = metadata?.request_id ?? crypto.randomUUID();
    const chunkId   = `chatcmpl-${requestId}`;
    const created   = Math.floor(Date.now() / 1000);

    const enc = new TextEncoder();

    /** Wrap an inner event as a Chat Completions SSE chunk. */
    function sseChunk(innerEvt, finishReason) {
      const obj = {
        id: chunkId,
        object: 'chat.completion.chunk',
        created,
        model,
        choices: [{
          index: 0,
          delta: { content: JSON.stringify(innerEvt) },
          finish_reason: finishReason ?? null,
        }],
      };
      return enc.encode(`data: ${JSON.stringify(obj)}\n\n`);
    }

    let streamController;
    const readable = new ReadableStream({
      start(c) { streamController = c; },
    });

    const pipePromise = (async () => {
      try {
        // Role chunk (standard Chat Completions preamble)
        streamController.enqueue(enc.encode(`data: ${JSON.stringify({
          id: chunkId, object: 'chat.completion.chunk', created, model,
          choices: [{ index: 0, delta: { role: 'assistant' }, finish_reason: null }],
        })}\n\n`));

        // init event
        streamController.enqueue(sseChunk({
          type: 'init',
          status: [101, 204, 205, 304].includes(targetRes.status) ? 200 : targetRes?.status ?? 200,
          headers: Object.fromEntries(targetRes.headers.entries()),
          binary: isBinary,
          model,
          request_id: requestId,
          target_url: targetUrl,
        }));

        if (targetRes.body) {
          const reader = targetRes.body.getReader();
          const dec = new TextDecoder();
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            streamController.enqueue(sseChunk(isBinary
              ? { type: 'bin',  chunk: u8ToBase64(value) }
              : { type: 'text', chunk: dec.decode(value, { stream: true }) }
            ));
          }
        }

        // done event with finish_reason
        streamController.enqueue(sseChunk(
          { type: 'done', ms: Date.now() - start },
          'stop'
        ));

        // [DONE] sentinel
        streamController.enqueue(enc.encode('data: [DONE]\n\n'));
      } catch (err) {
        try { streamController.enqueue(sseChunk({ type: 'error', message: err.message })); } catch {}
        try { streamController.enqueue(enc.encode('data: [DONE]\n\n')); } catch {}
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

/** Error response in Chat Completions SSE format. Always emits init first. */
function chatError(model, message) {
  const chunkId = `chatcmpl-${crypto.randomUUID()}`;
  const created = Math.floor(Date.now() / 1000);
  const mdl     = model ?? 'unknown';

  function chunk(content, finishReason) {
    return `data: ${JSON.stringify({
      id: chunkId, object: 'chat.completion.chunk', created, model: mdl,
      choices: [{ index: 0, delta: { content: JSON.stringify(content) }, finish_reason: finishReason ?? null }],
    })}\n\n`;
  }

  const lines = [
    chunk({ type: 'init', status: 502, headers: {}, binary: false, model: mdl, request_id: null }),
    chunk({ type: 'text', chunk: message }),
    chunk({ type: 'done', ms: 0 }, 'stop'),
    'data: [DONE]\n\n',
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
