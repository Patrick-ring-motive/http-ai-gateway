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
  let bodyText = null;
  let isBase64 = false;
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    const ct = request.headers.get('content-type') ?? '';
    if (IS_TEXT.test(ct)) {
      bodyText = await request.text();
    } else {
      const buf = await request.arrayBuffer();
      bodyText = u8ToBase64(new Uint8Array(buf));
      isBase64 = true;
    }
  }

  const filteredHeaders = Object.fromEntries(
    [...request.headers.entries()].filter(([k]) => !HOP_BY_HOP.has(k.toLowerCase()))
  );
  filteredHeaders['outbound-api-key'] = String(env.OUTBOUND_API_KEY);

  // ── Chat Completions envelope (flat KV messages) ────────────────────────
  const messages = [{
      role: 'user',
      name: 'method',
      content: request.method
    },
    {
      role: 'user',
      name: 'url',
      content: request.url
    },
    {
      role: 'user',
      name: 'path',
      content: url.pathname
    },
    {
      role: 'user',
      name: 'search',
      content: url.search
    },
    {
      role: 'user',
      name: 'request_id',
      content: requestId
    },
    {
      role: 'user',
      name: 'content_type',
      content: request.headers.get('content-type') ?? ''
    },
    {
      role: 'user',
      name: 'is_base64',
      content: String(isBase64)
    },
    {
      role: 'user',
      name: 'timestamp',
      content: new Date().toISOString()
    },
  ];
  for (const [k, v] of Object.entries(filteredHeaders)) {
    messages.push({
      role: 'system',
      name: k,
      content: v
    });
  }
  if (bodyText != null) {
    messages.push({
      role: 'user',
      name: 'body',
      content: bodyText
    });
  }

  const envelope = {
    model: modelName,
    messages,
    stream: true,
  };

  if (!env.AI_GATEWAY_URL) {
    return new Response('AI_GATEWAY_URL not configured', {
      status: 500,
      headers: {
        'content-type': 'text/html'
      }
    });
  }

  let gatewayRes;
  try {
    gatewayRes = await fetch(env.AI_GATEWAY_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Request-Id': requestId,
        ...(env.AI_GATEWAY_TOKEN ? {
          'cf-aig-authorization': `Bearer ${env.AI_GATEWAY_TOKEN}`
        } : {}),
        'User-Agent': String(env.TARGET_BASE_URL)
      },
      body: JSON.stringify(envelope),
    });
  } catch (err) {
    return new Response(`Gateway unreachable: ${err.message}`, {
      status: 502,
      headers: {
        'content-type': 'text/html'
      }
    });
  }

  if (gatewayRes.status >= 400) {
    const errBody = await gatewayRes.text();
    return new Response(`Gateway error ${gatewayRes.status}: ${errBody}`, {
      status: 502,
      headers: {
        'content-type': 'text/html'
      }
    });
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
  const dec = new TextDecoder();
  const enc = new TextEncoder();
  let buf = '';
  let streamDone = false;
  const pendingChunks = [];

  /** Parse a Chat Completions SSE line into {role, content}, or null. */
  function parseChunkLine(line) {
    const trimmed = line.trim();
    if (!trimmed.startsWith('data:')) return [];
    const payload = trimmed.slice(5).trim();
    if (payload === '[DONE]') return [];
    try {
      const msgs = [];
      const chunk = JSON.parse(payload);
      for (const choice of chunk?.choices ?? []) {
        const delta = choice?.delta;
        if (!delta?.role) continue;
        msgs.push({
          role: delta.role,
          content: delta.content ?? '',
          name: delta?.tool_calls?.[0]?.["function"]?.name ?? ''
        });
      }
      return msgs;
    } catch {
      return [];
    }
  }

  /** Drain complete SSE events from buf and return parsed {role, content} objects. */
  function drainEvents() {
    const parts = buf.split('\n\n');
    buf = parts.pop() ?? '';
    const out = [];
    for (const part of parts) {
      for (const line of part.split('\n')) {
        const evts = parseChunkLine(line);
        out.push(...evts);
      }
    }
    return out;
  }

  // Accumulate init fields until role='init' signals completion.
  const init = {
    status: 502,
    headers: {},
    bytes: false
  };
  let initDone = false;
  while (!initDone) {
    const {
      done,
      value
    } = await reader.read();
    if (done) {
      initDone = true;
      streamDone = true;
      break;
    }
    buf += dec.decode(value, {
      stream: true
    });
    for (const {
        role,
        content,
        name
      }
      of drainEvents()) {
      if (role === 'system') {
        init.headers[name] = content;
        continue;
      }
      if (role !== 'assistant') continue;
      switch (name) {
        case 'status':
          init.status = parseInt(content, 10) || 502;
          break;
        case 'bytes':
          init.bytes = content === 'true';
          break;
        case 'init':
          initDone = true;
          break;
        case 'text':
          pendingChunks.push(enc.encode(content));
          break;
        case 'blob':
          pendingChunks.push(base64ToU8(content));
          break;
        case 'error':
          pendingChunks.push(enc.encode(content || 'upstream error'));
          break;
      }
    }
  }

  const readable = new ReadableStream({
    start(controller) {
      for (const chunk of pendingChunks) controller.enqueue(chunk);
      if (streamDone) controller.close();
    },
    async pull(controller) {
      while (true) {
        const {
          done,
          value
        } = await reader.read();
        if (done) {
          controller.close();
          return;
        }
        buf += dec.decode(value, {
          stream: true
        });
        let enqueued = false;
        for (const {
            name,
            content
          }
          of drainEvents()) {
          switch (name) {
            case 'text':
              controller.enqueue(enc.encode(content));
              enqueued = true;
              break;
            case 'blob':
              controller.enqueue(base64ToU8(content));
              enqueued = true;
              break;
            case 'error':
              controller.enqueue(enc.encode(content || 'upstream error'));
              enqueued = true;
              break;
          }
        }
        if (enqueued) return;
      }
    },
  });

  const resHeaders = new Headers();
  for (const [k, v] of Object.entries(init.headers ?? {})) {
    try {
      if (!HOP_BY_HOP.has(k.toLowerCase())) resHeaders.set(k, v);
    } catch {}
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

// ── codec ──────────────────────────────────────────────────────────────────────

function u8ToBase64(u8) {
  let s = '';
  const len = u8.length;
  for (let i = 0; i !== len; ++i) s += String.fromCharCode(u8[i]);
  return btoa(s);
}

function base64ToU8(b64) {
  const blob = atob(b64);
  const u8 = new Uint8Array(blob.length);
  const len = blob.length;
  for (let i = 0; i !== len; ++i) u8[i] = blob.charCodeAt(i);
  return u8;
}
