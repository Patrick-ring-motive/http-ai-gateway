/**
 * worker-outbound-oai — RIGHT of AI Gateway (OpenAI Chat Completions format)
 *
 * POST (Chat Completions envelope) → HTTP request to real target
 * HTTP ReadableStream → SSE (Chat Completions chunks) back through AI Gateway
 *
 * Request metadata arrives as flat KV messages:
 *   {role:'method', content:'GET'}, {role:'path', content:'/foo'}, ...
 * The request body arrives in the message with role='user'.
 *
 * Response chunks use delta.role as the event type and delta.content
 * as the value — no nested JSON. Init fields sent individually,
 * terminated by {role:'init', content:'done'}.
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

    // Extract metadata from flat KV messages
    const metadata = { headers: {} };
    let input = null;
    for (const msg of messages) {
      switch (msg.role) {
        case 'body':         input = msg.content; break;
        case 'header':       metadata.headers[msg.name] = msg.content; break;
        case 'method':       metadata.method = msg.content; break;
        case 'url':          metadata.url = msg.content; break;
        case 'path':         metadata.path = msg.content; break;
        case 'search':       metadata.search = msg.content; break;
        case 'request_id':   metadata.request_id = msg.content; break;
        case 'content_type': metadata.content_type = msg.content; break;
        case 'is_base64':    metadata.is_base64 = msg.content === 'true'; break;
        case 'timestamp':    metadata.timestamp = msg.content; break;
      }
    }

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

    /** Emit a single SSE chunk with delta.role and delta.content. */
    function sseChunk(role, content, finishReason) {
      const obj = {
        id: chunkId,
        object: 'chat.completion.chunk',
        created,
        model,
        choices: [{
          index: 0,
          delta: { role, content },
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
        const status = [101, 204, 205, 304].includes(targetRes.status) ? 200 : targetRes?.status ?? 200;

        // Init fields as individual KV chunks
        streamController.enqueue(sseChunk('status',     String(status)));
        for (const [k, v] of targetRes.headers.entries()) {
          streamController.enqueue(sseChunk(k, v));
        }
        streamController.enqueue(sseChunk('binary',     String(isBinary)));
        streamController.enqueue(sseChunk('model',      model));
        streamController.enqueue(sseChunk('request_id', requestId));
        streamController.enqueue(sseChunk('target_url', targetUrl));
        streamController.enqueue(sseChunk('init',       'done'));

        if (targetRes.body) {
          const reader = targetRes.body.getReader();
          const dec = new TextDecoder();
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            streamController.enqueue(sseChunk(
              isBinary ? 'bin' : 'text',
              isBinary ? u8ToBase64(value) : dec.decode(value, { stream: true })
            ));
          }
        }

        // done event with finish_reason
        streamController.enqueue(sseChunk('done', String(Date.now() - start), 'stop'));

        // [DONE] sentinel
        streamController.enqueue(enc.encode('data: [DONE]\n\n'));
      } catch (err) {
        try { streamController.enqueue(sseChunk('error', err.message)); } catch {}
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

  function chunk(role, content, finishReason) {
    return `data: ${JSON.stringify({
      id: chunkId, object: 'chat.completion.chunk', created, model: mdl,
      choices: [{ index: 0, delta: { role, content }, finish_reason: finishReason ?? null }],
    })}\n\n`;
  }

  const lines = [
    chunk('status', '502'),
    chunk('binary', 'false'),
    chunk('model', mdl),
    chunk('init', 'done'),
    chunk('text', message),
    chunk('done', '0', 'stop'),
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
