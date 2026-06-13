/**
 * worker-inbound — LEFT of AI Gateway
 *
 * HTTP request → Responses API envelope → AI Gateway
 * SSE stream (from worker-outbound via gateway) → HTTP ReadableStream → client
 *
 * Waits for the 'init' SSE event to learn status + headers, then returns a
 * streaming Response whose body is piped live from subsequent text/blob events.
 * bytes chunks are individually base64-decoded and written as raw bytes.
 */

const IS_TEXT = /text|html|script|xml|json|pdf/i;

const HOP_BY_HOP = new Set([
  'connection','keep-alive','proxy-authenticate','proxy-authorization',
  'te','trailers','transfer-encoding','upgrade',
  'host','cf-connecting-ip','cf-ray','cf-visitor','cf-ipcountry',
  'x-forwarded-for','x-forwarded-proto','x-real-ip',
]);

export default {
    async fetch(...args){
      try{
        return await onRequest(...args);
      }catch(e){
        console.warn(e);
        return new Response(String(e),{status:500,statusText:String(e)});
      }
    }
}
  async function onRequest(request, env) {
    const url      = new URL(request.url);
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
    filteredHeaders['outbound-api-key'] = String(env.OUTBOUND_API_KEY)
    const envelope = {
      model: modelName,
      input: bodyText,
      stream: true,          // signal to gateway that response will be SSE
      metadata: {
        method: request.method,
        url: request.url,
        path: url.pathname,
        search: url.search,
        headers: filteredHeaders,
        request_id: requestId,
        content_type: request.headers.get('content-type') ?? '',
        is_base64: isBase64,
        timestamp: new Date().toISOString(),
      },
    };

    if (!env.AI_GATEWAY_URL) {
      return new Response('AI_GATEWAY_URL not configured', { status: 500 ,headers:{'content-type':'text/html'}});
    }

    let gatewayRes;
    try {
      gatewayRes = await fetch(env.AI_GATEWAY_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Request-Id': requestId,
          ...(env.AI_GATEWAY_TOKEN
            ? { 'cf-aig-authorization': `Bearer ${env.AI_GATEWAY_TOKEN}` }
            : {}),
          'User-Agent':String(env.TARGET_BASE_URL)
        },
        body: JSON.stringify(envelope),
      });
    } catch (err) {
      return new Response(`Gateway unreachable: ${err.message}`, { status: 502 ,headers:{'content-type':'text/html'}});
    }

    if (gatewayRes.status >= 400) {
      const errBody = await gatewayRes.text();
      return new Response(`Gateway error ${gatewayRes.status}: ${errBody}`, { status: 502 ,headers:{'content-type':'text/html'}});
    }
    if(!gatewayRes.body){
      return gatewayRes;
    }
    // ── SSE → HTTP ReadableStream ─────────────────────────────────────────────
    //
    // We must know status+headers before returning a Response, so we read SSE
    // events inline until 'init' arrives, then hand remaining data to a
    // pull()-based ReadableStream so the runtime always tracks the I/O.

    const reader  = gatewayRes.body.getReader();
    const dec     = new TextDecoder();
    const enc     = new TextEncoder();
    let buf       = '';
    let streamDone = false;
    const pendingChunks = [];

    /** Drain complete SSE events from buf and return parsed objects. */
    function drainEvents() {
      const parts = buf.split('\n\n');
      buf = parts.pop() ?? '';
      const out = [];
      for (const part of parts) {
        const trimmed = part.trim();
        if (!trimmed.startsWith('data:')) continue;
        try { out.push(JSON.parse(trimmed.slice(5).trim())); }
        catch { /* skip malformed */ }
      }
      return out;
    }

    // Read SSE until we receive the init event (status + headers).
    // Any data chunks that arrive in the same batch are buffered.
    let init;
    while (!init) {
      const { done, value } = await reader.read();
      if (done) {
        init = { status: 502, headers: {}, bytes: false };
        streamDone = true;
        break;
      }
      buf += dec.decode(value, { stream: true });
      for (const evt of drainEvents()) {
        switch (evt.type) {
          case 'init':
            init = evt;
            break;                 // switch-break; loop continues to drain batch
          case 'text':
            pendingChunks.push(enc.encode(evt.chunk));
            break;
          case 'blob':
            pendingChunks.push(base64ToU8(evt.chunk));
            break;
          case 'error':
            if (!init) init = { status: 502, headers: {}, bytes: false };
            pendingChunks.push(enc.encode(evt.message ?? 'upstream error'));
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
          const { done, value } = await reader.read();
          if (done) { controller.close(); return; }
          buf += dec.decode(value, { stream: true });
          let enqueued = false;
          for (const evt of drainEvents()) {
            switch (evt.type) {
              case 'text':
                controller.enqueue(enc.encode(evt.chunk));
                enqueued = true;
                break;
              case 'blob':
                controller.enqueue(base64ToU8(evt.chunk));
                enqueued = true;
                break;
              case 'error':
                controller.enqueue(enc.encode(evt.message ?? 'upstream error'));
                enqueued = true;
                break;
            }
          }
          if (enqueued) return;   // respect backpressure
        }
      },
    });

    const resHeaders = new Headers();
    for (const [k, v] of Object.entries(init.headers ?? {})) {
      if (!HOP_BY_HOP.has(k.toLowerCase())) resHeaders.set(k, v);
    }
    resHeaders.set('x-bridge-request-id', requestId);
    resHeaders.set('x-bridge-model', modelName);
    if(/^(text\/plain|null|undefined)|json/i.test(resHeaders.get('content-type'))){
      resHeaders.set('content-type','text/html');
    }
    if(/script/i.test(resHeaders.get('content-type'))){
      if(request.headers.get('accept') !== `*/*`){
        resHeaders.set('content-type','text/html');
      }
    }
    if(/^text\/css/i.test(resHeaders.get('content-type'))){
      if(!/^text\/css/i.test(request.headers.get('accept'))){
        resHeaders.set('content-type','text/html');
      }
    }
    // Return streaming response — body fills as SSE chunks arrive
    return new Response(readable, {
      status: init.status,
      headers: resHeaders,
    });
};

function u8ToBase64(u8) {
  let s = '';
  const len = u8.length;
  for (let i = 0; i !== len; ++i) s += String.fromCharCode(u8[i]);
  return btoa(s);
}

function base64ToU8(b64) {
  const blob = atob(b64);
  const u8  = new Uint8Array(blob.length);
  const len = blob.length;
  for (let i = 0; i !== len; ++i) u8[i] = blob.charCodeAt(i);
  return u8;
}
