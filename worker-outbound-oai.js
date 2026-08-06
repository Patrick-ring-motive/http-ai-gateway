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

const ENCODER = new TextEncoder(); // stateless — safe to share across requests

const newResponse = (...args) => {
  try {
    return new Response(...args);
  } catch (e) {
    if (e?.message === 'Response with null body status (101, 204, 205, or 304) cannot have a body.') {
      return new Response(null, ...args.slice(1));
    }
    return new Response(String(e), {
      status: 500,
      statusText: String(e)
    });
  }
};

const IS_TEXT = /text|html|script|xml|json|pdf/i;

const HOP_BY_HOP = new Set([
  'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
  'te', 'trailers', 'transfer-encoding', 'upgrade',
  'host', 'cf-connecting-ip', 'cf-ray', 'cf-visitor', 'cf-ipcountry',
]);

const isHopByHop = (key) => HOP_BY_HOP.has(key.toLowerCase());

/** Shared default for SSE responses. Change headers here, not per call site. */
const DEFAULT_SSE_HEADERS = {
  'Content-Type': 'text/event-stream',
  'Cache-Control': 'no-cache',
};

// Flat KV message name → metadata field is a straight copy by default;
// this only needs to list the exceptions.
const META_TRANSFORMS = {
  is_base64: (content) => content === 'true',
};

export default {
  async fetch(request, env, ctx) {
    if (request.headers.get('authorization') !== `Bearer ${env.CF_AIG_AUTHORIZATION}`) {
      return chatErrorResponse(null, 'AI Gateway unauthorized');
    }

    let payload;
    try {
      payload = await request.json();
    } catch (e) {
      console.warn(e);
      return chatErrorResponse(null, 'Invalid JSON body');
    }

    const { model, messages } = payload;
    if (!model) return chatErrorResponse(null, 'Missing model field');
    if (!messages?.length) return chatErrorResponse(model, 'Missing messages array');

    // Extract metadata from flat KV messages. Most fields are a straight
    // name → content copy; META_TRANSFORMS covers the exceptions.
    const metadata = { headers: {} };
    let input = null;
    for (const msg of messages) {
      if (msg.role === 'system') {
        metadata.headers[msg.name] = msg.content;
        continue;
      }
      if (msg.role !== 'user') continue;
      if (msg.name === 'body') {
        input = msg.content;
        continue;
      }
      const transform = META_TRANSFORMS[msg.name];
      metadata[msg.name] = transform ? transform(msg.content) : msg.content;
    }

    const targetUrl = resolveTargetUrl(model, metadata, env);
    if (!targetUrl) return chatErrorResponse(model, `No route for model: ${model}`);

    const fwdHeaders = new Headers();
    for (const key in metadata.headers) {
      if (!isHopByHop(key)) fwdHeaders.set(key, metadata.headers[key]);
    }

    if (fwdHeaders.get('outbound-api-key') !== env.OUTBOUND_API_KEY) {
      return chatErrorResponse(null, 'Missing outbound api key');
    }

    const method = (metadata.method ?? 'GET').toUpperCase();
    let fwdBody;
    if (!/head|get/i.test(method) && input != null) {
      fwdBody = metadata.is_base64 ? base64ToBytes(input) : input;
    }

    const start = Date.now();
    let targetRes;
    try {
      targetRes = await fetch(targetUrl, {
        method,
        headers: fwdHeaders,
        body: fwdBody,
        redirect: 'follow',
      });
    } catch (err) {
      return chatErrorResponse(model, `Target unreachable: ${err.message}`);
    }

    const isBytes = !IS_TEXT.test(targetRes.headers.get('content-type'));

    const requestId = metadata.request_id ?? crypto.randomUUID();
    const chunkId = `chatcmpl-${requestId}`;
    const created = Math.floor(Date.now() / 1000);

    let streamController;
    const readable = new ReadableStream({
      start(controller) {
        streamController = controller;
      },
    });

    const pipePromise = (async () => {
      try {
        const status = [101, 204, 205, 304].includes(targetRes.status) ? 200 : targetRes.status;

        // Init fields as individual KV chunks, batched into one SSE line.
        const initChunks = [['status', String(status)]];
        for (const [key, value] of targetRes.headers) {
          initChunks.push([key, value, null, 'system']);
        }
        initChunks.push(
          ['bytes', String(isBytes)],
          ['model', model],
          ['request_id', requestId],
          ['target_url', targetUrl],
          ['init', 'done'],
        );
        streamController.enqueue(ENCODER.encode(sseLine(chunkId, created, model, initChunks)));

        if (targetRes.body) {
          const reader = targetRes.body.getReader();
          const decoder = new TextDecoder();
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            const content = isBytes ? bytesToBase64(value) : decoder.decode(value, { stream: true });
            const name = isBytes ? 'blob' : 'text';
            streamController.enqueue(ENCODER.encode(sseLine(chunkId, created, model, [[name, content]])));
          }
        }

        // done event with finish_reason
        streamController.enqueue(
          ENCODER.encode(sseLine(chunkId, created, model, [['done', String(Date.now() - start), 'stop']]))
        );

        // [DONE] sentinel
        streamController.enqueue(ENCODER.encode('data: [DONE]\n\n'));
      } catch (err) {
        try {
          streamController.enqueue(ENCODER.encode(sseLine(chunkId, created, model, [['error', err.message]])));
          streamController.enqueue(ENCODER.encode('data: [DONE]\n\n'));
        } catch (e) {
          console.warn(e);
        }
      } finally {
        try {
          streamController.close();
        } catch (e) {
          console.warn(e);
        }
      }
    })();

    ctx.waitUntil(pipePromise);

    return newResponse(readable, {
      status: targetRes.status,
      headers: { ...DEFAULT_SSE_HEADERS, 'X-Accel-Buffering': 'no' },
    });
  },
};

// ── routing ──────────────────────────────────────────────────────────────

function resolveTargetUrl(model, metadata, env) {
  if (env.ROUTES) {
    let table;
    try {
      table = JSON.parse(env.ROUTES);
    } catch (e) {
      console.warn('bad ROUTES JSON', e);
    }
    if (table) {
      if (table[model]) return buildUrl(table[model], metadata);
      let best = null;
      let bestLen = -1;
      for (const prefix in table) {
        if (model.startsWith(prefix + '.') && prefix.length > bestLen) {
          best = table[prefix];
          bestLen = prefix.length;
        }
      }
      if (best) return buildUrl(best, metadata);
    }
  }
  if (env.TARGET_BASE_URL) return buildUrl(env.TARGET_BASE_URL, metadata);

  const segs = (metadata.path ?? '').replace(/^\//, '').split('/');
  if (segs[0]?.includes('.')) {
    const rest = segs.length > 1 ? '/' + segs.slice(1).join('/') : '/';
    return `https://${segs[0]}${rest}${metadata.search ?? ''}`;
  }

  return null;
}

function buildUrl(base, metadata) {
  return base.replace(/\/+$/, '') + (metadata.path ?? '') + (metadata.search ?? '');
}

// ── SSE helpers ──────────────────────────────────────────────────────────

function toChoice([name, content, finishReason, role = 'assistant'], index) {
  return {
    index,
    delta: { role, content, tool_calls: [{ index: 0, function: { name } }] },
    finish_reason: finishReason ?? null,
  };
}

/**
 * One SSE `data: {...}\n\n` line covering one or more choices.
 * entries: array of [name, content, finishReason?, role?] tuples.
 * Shared by the main stream and chatErrorResponse — same wire shape either way.
 */
function sseLine(chunkId, created, model, entries) {
  const choices = entries.map(toChoice);
  return `data: ${JSON.stringify({ id: chunkId, object: 'chat.completion.chunk', created, model, choices })}\n\n`;
}

/** Error response in Chat Completions SSE format. Always emits init first. */
function chatErrorResponse(model, message) {
  const chunkId = `chatcmpl-${crypto.randomUUID()}`;
  const created = Math.floor(Date.now() / 1000);
  const modelName = model ?? 'unknown';

  const lines = [
    sseLine(chunkId, created, modelName, [
      ['status', '502'],
      ['bytes', 'false'],
      ['model', modelName],
      ['init', 'done'],
      ['text', message],
      ['done', '0', 'stop'],
    ]),
    'data: [DONE]\n\n',
  ].join('');

  return newResponse(lines, {
    status: 500,
    headers: DEFAULT_SSE_HEADERS,
  });
}

// ── codec ────────────────────────────────────────────────────────────────

function bytesToBase64(bytes) {
  let str = '';
  const len = bytes.length;
  for (let i = 0; i !== len; ++i) str += String.fromCharCode(bytes[i]);
  return btoa(str);
}

function base64ToBytes(base64) {
  const binaryString = atob(base64);
  const bytes = new Uint8Array(binaryString.length);
  const len = binaryString.length;
  for (let i = 0; i !== len; ++i) bytes[i] = binaryString.charCodeAt(i);
  return bytes;
}
