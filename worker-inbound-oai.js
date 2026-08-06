/**
 * worker-inbound-oai — LEFT of AI Gateway (OpenAI Chat Completions format)
 *
 * HTTP request → Chat Completions envelope → AI Gateway
 * SSE stream (Chat Completions chunks from upstream-worker-oai via gateway)
 *   → HTTP ReadableStream → client
 *
 * Request metadata is sent as flat KV messages:
 *   {role:'method', content:'GET'}, {role:'path', content:'/foo'}, ...
 * The request body travels in a user message.
 *
 * Response chunks use delta.role as the event type and delta.content
 * as the value — no nested JSON. Init fields (status, headers, bytes)
 * arrive individually, terminated by {role:'init', content:'done'}.
 */

const IS_TEXT = /text|html|script|xml|json|pdf/i;

const HOP_BY_HOP = new Set([
  'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
  'te', 'trailers', 'transfer-encoding', 'upgrade',
  'host', 'cf-connecting-ip', 'cf-ray', 'cf-visitor', 'cf-ipcountry',
  'x-forwarded-for', 'x-forwarded-proto', 'x-real-ip', 'body'
]);

const isHopByHop = (key) => HOP_BY_HOP.has(key.toLowerCase());

/** Plain-text/html error response — the shape every early-exit path in onRequest needs. */
const errorResponse = (message, status) =>
  new Response(message, { status, headers: { 'content-type': 'text/html' } });

export default {
  async fetch(...args) {
    try {
      return await onRequest(...args);
    } catch (e) {
      console.warn(e);
      return new Response(String(e), {
        status: 500,
        statusText: String(e)
      });
    }
  },
};

async function onRequest(request, env) {
  const url = new URL(request.url);
  const requestId = request.headers.get('x-request-id') ?? crypto.randomUUID();

  const modelName =
    url.pathname.replace(/^\/+|\/+$/g, '').replace(/\//g, '.') ||
    env.DEFAULT_MODEL ||
    'default';

  // Read request body
  const contentType = request.headers.get('content-type');
  let bodyText = null;
  let isBase64 = false;
  if (!/head|get/i.test(request.method)) {
    if (IS_TEXT.test(contentType)) {
      bodyText = await request.text();
    } else {
      const arrayBuffer = await request.arrayBuffer();
      bodyText = bytesToBase64(new Uint8Array(arrayBuffer));
      isBase64 = true;
    }
  }

  // Filter hop-by-hop headers once, reuse for outbound envelope.
  const filteredHeaders = {};
  for (const [key, value] of request.headers) {
    if (!isHopByHop(key)) filteredHeaders[key] = value;
  }
  filteredHeaders['outbound-api-key'] = String(env.OUTBOUND_API_KEY);

  // ── Chat Completions envelope (flat KV messages) ────────────────────────
  const requestMeta = {
    method: request.method,
    url: request.url,
    path: url.pathname,
    search: url.search,
    request_id: requestId,
    content_type: contentType ?? '',
    is_base64: String(isBase64),
    timestamp: new Date().toISOString(),
  };
  const messages = [];
  for (const name in requestMeta) {
    messages.push({ role: 'user', name, content: requestMeta[name] });
  }
  for (const key in filteredHeaders) {
    messages.push({ role: 'system', name: key, content: filteredHeaders[key] });
  }
  if (bodyText != null) {
    messages.push({ role: 'user', name: 'body', content: bodyText });
  }

  const envelope = {
    model: modelName,
    messages,
    stream: true,
  };

  if (!env.AI_GATEWAY_URL) {
    return errorResponse('AI_GATEWAY_URL not configured', 500);
  }

  let gatewayRes;
  try {
    gatewayRes = await fetch(env.AI_GATEWAY_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Request-Id': requestId,
        ...(env.AI_GATEWAY_TOKEN ? { 'cf-aig-authorization': `Bearer ${env.AI_GATEWAY_TOKEN}` } : {}),
        'User-Agent': String(env.TARGET_BASE_URL)
      },
      body: JSON.stringify(envelope),
    });
  } catch (err) {
    return errorResponse(`Gateway unreachable: ${err.message}`, 502);
  }

  if (gatewayRes.status >= 400) {
    const errBody = await gatewayRes.text();
    return errorResponse(`Gateway error ${gatewayRes.status}: ${errBody}`, 502);
  }
  if (!gatewayRes.body) {
    return gatewayRes;
  }

  // ── SSE (Chat Completions chunks) → HTTP ReadableStream ─────────────────
  //
  // Each SSE chunk carries delta.role (event type) and delta.content (value).
  // Init fields (status, headers, bytes) arrive as individual KV chunks,
  // terminated by role='init'. Then text/blob chunks stream the body.

  const reader = gatewayRes.body.getReader();
  const decoder = new TextDecoder(); // per-request: keeps streaming state across reads
  let buffer = '';
  let streamDone = false;
  const pendingChunks = [];

  /** Parse a Chat Completions SSE line into [{role, content, name}], or []. */
  function parseChunkLine(line) {
    const trimmed = line.trim();
    if (!trimmed.startsWith('data:')) return [];
    const payload = trimmed.slice(5).trim();
    if (payload === '[DONE]') return [];
    try {
      const chunk = JSON.parse(payload);
      const parsed = [];
      for (const choice of chunk?.choices ?? []) {
        const delta = choice?.delta;
        if (!delta?.role) continue;
        parsed.push({
          role: delta.role,
          content: delta.content ?? '',
          name: delta?.tool_calls?.[0]?.['function']?.name ?? ''
        });
      }
      return parsed;
    } catch (e) {
      console.warn('parseChunkLine', e);
      return [];
    }
  }

  /** Drain complete SSE events from buffer, return parsed {role, content, name} objects. */
  function drainEvents() {
    const parts = buffer.split('\n\n');
    buffer = parts.pop() ?? '';
    const events = [];
    for (const part of parts) {
      for (const line of part.split('\n')) {
        events.push(...parseChunkLine(line));
      }
    }
    return events;
  }

  // Accumulate init fields until role='init' signals completion.
  const init = { status: 502, headers: {}, bytes: false };
  let initDone = false;
  while (!initDone) {
    const { done, value } = await reader.read();
    if (done) {
      initDone = true;
      streamDone = true;
      break;
    }
    buffer += decoder.decode(value, { stream: true });

    for (const { role, content, name } of drainEvents()) {
      if (role === 'system') {
        init.headers[name] = content;
        continue;
      }
      if (role !== 'assistant') continue;

      if (name === 'status') { init.status = parseInt(content, 10) || 502; continue; }
      if (name === 'bytes') { init.bytes = content === 'true'; continue; }
      if (name === 'init') { initDone = true; continue; }

      const bytes = chunkToBytes(name, content);
      if (bytes) pendingChunks.push(bytes);
    }
  }

  const readable = new ReadableStream({
    start(controller) {
      for (const chunk of pendingChunks) controller.enqueue(chunk);
      if (streamDone) controller.close();
    },
    async pull(controller) {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          controller.close();
          return;
        }
        buffer += decoder.decode(value, { stream: true });

        let enqueued = false;
        for (const { name, content } of drainEvents()) {
          const bytes = chunkToBytes(name, content);
          if (bytes) {
            controller.enqueue(bytes);
            enqueued = true;
          }
        }
        if (enqueued) return;
      }
    },
  });

  const resHeaders = new Headers();
  for (const key in init.headers) {
    if (isHopByHop(key)) continue;
    try {
      resHeaders.set(key, init.headers[key]);
    } catch (e) {
      console.warn(e);
    }
  }
  resHeaders.set('x-bridge-request-id', requestId);
  resHeaders.set('x-bridge-model', modelName);
  if (/^(?:text\/plain|undefined)/i.test(resHeaders.get('content-type'))) {
    resHeaders.set('content-type', 'text/html');
  }

  return new Response(readable, {
    status: init.status,
    headers: resHeaders,
  });
}

// ── codec ────────────────────────────────────────────────────────────────

const ENCODER = new TextEncoder(); // stateless — safe to share across requests

// Data-driven dispatch: chunk name → bytes, replaces the duplicated
// text/blob/error switch that used to live in both the init loop and pull().
const CHUNK_DECODERS = {
  text: (content) => ENCODER.encode(content),
  blob: (content) => base64ToBytes(content),
  error: (content) => ENCODER.encode(content || 'upstream error'),
};

function chunkToBytes(name, content) {
  return CHUNK_DECODERS[name]?.(content) ?? null;
}

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
