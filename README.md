# HTTP AI Gateway

A bidirectional HTTP-to-SSE bridge for streaming AI responses through Cloudflare Workers. Designed to handle real-time streaming from any HTTP target while transparently managing request routing, content encoding, and header filtering.

## Architecture

The gateway consists of two complementary workers:

### `worker-inbound.js` — LEFT side
Receives HTTP requests from clients and forwards them to the AI gateway as JSON envelopes.

```
HTTP request → Responses API envelope → AI Gateway (SSE)
```

**Key features:**
- Transparent request capture (method, path, headers, body)
- Automatic content-type detection (text vs. binary)
- Base64 encoding for binary payloads
- SSE stream consumption from gateway
- Live streaming response to client

### `worker-outbound.js` — RIGHT side
Receives SSE from the gateway, forwards to real HTTP targets, and streams responses back as SSE.

```
POST envelope → HTTP request to target → HTTP response stream → SSE
```

**Key features:**
- Dynamic routing via config table or passthrough
- Automatic target URL resolution
- Binary-aware chunking
- Request/response streaming with proper backpressure

## Request Flow

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ HTTP request
       ▼
┌──────────────────────────────────────────┐
│  worker-inbound.js                       │
│  • Captures request metadata             │
│  • Reads body (text or binary)           │
│  • Creates Responses API envelope        │
│  • Sends to AI Gateway via SSE listener  │
└──────┬───────────────────────────────────┘
       │ POST JSON envelope
       ▼
┌──────────────────────────────────────────┐
│    AI Gateway                            │
│    (processes, routes, logs)             │
└──────┬───────────────────────────────────┘
       │ SSE stream (init, text/bin, done)
       ▼
┌──────────────────────────────────────────┐
│  worker-outbound.js                      │
│  • Resolves target URL from model        │
│  • Forwards request to target            │
│  • Encodes response as SSE               │
│  • Streams back through gateway          │
└──────┬───────────────────────────────────┘
       │ SSE stream
       ▼
┌──────────────────────────────────────────┐
│  worker-inbound.js (ReadableStream)      │
│  • Consumes SSE events                   │
│  • Decodes base64 chunks                 │
│  • Pipes to Response body                │
└──────┬───────────────────────────────────┘
       │ HTTP Response (streaming)
       ▼
┌─────────────┐
│   Client    │ ← receives streamed data
└─────────────┘
```

## Configuration

### Environment Variables

#### `worker-inbound.js`
- **`AI_GATEWAY_URL`** (required): Full URL to the AI gateway's inbound endpoint
- **`AI_GATEWAY_TOKEN`** (optional): Bearer token for gateway authorization
- **`OUTBOUND_API_KEY`** (required): Secret key for outbound worker validation
- **`DEFAULT_MODEL`** (optional): Fallback model when path doesn't specify one

#### `worker-outbound.js`
- **`CF_AIG_AUTHORIZATION`** (required): Bearer token matching inbound's `AI_GATEWAY_TOKEN`
- **`OUTBOUND_API_KEY`** (required): Secret key for inbound validation
- **`ROUTES`** (optional): JSON map of model prefixes to target URLs
- **`TARGET_BASE_URL`** (optional): Fallback base URL for all requests

### Request Envelope

The inbound worker sends requests to the gateway in this format:

```json
{
  "model": "string (model name or path)",
  "input": "string (request body, base64 if binary)",
  "stream": true,
  "metadata": {
    "method": "GET|POST|etc",
    "url": "full original URL",
    "path": "/original/path",
    "search": "?query=string",
    "headers": { "key": "value" },
    "request_id": "uuid",
    "content_type": "application/json",
    "is_base64": false,
    "timestamp": "2024-01-01T00:00:00.000Z"
  }
}
```

### SSE Event Schema

The outbound worker responds (and streams back through inbound) with SSE events:

```javascript
// Init — sent first, contains response metadata
{ type: 'init', status: 200, headers: {}, binary: false, model: 'gpt-4', request_id: 'uuid', target_url: 'https://...' }

// Text chunk
{ type: 'text', chunk: 'string content' }

// Binary chunk (base64-encoded)
{ type: 'bin', chunk: 'base64string' }

// Completion
{ type: 'done', ms: 1234 }

// Error
{ type: 'error', message: 'error description' }
```

## Routing

The outbound worker resolves target URLs in this order:

### 1. **Explicit Routing Table** (`ROUTES` env var)
```json
{
  "openai": "https://api.openai.com",
  "anthropic": "https://api.anthropic.com",
  "deepseek.v3": "https://api.deepseek.com"
}
```

Supports prefix matching: `/deepseek.v3.chat` → `https://api.deepseek.com/chat`

### 2. **Fallback Base URL** (`TARGET_BASE_URL` env var)
Used if no routing table entry matches.

### 3. **Zero-Config Passthrough**
If the first path segment contains a dot, it's treated as a hostname:

```
/en.wikipedia.org/w/api.php → https://en.wikipedia.org/w/api.php
/api.openai.com/v1/chat/completions → https://api.openai.com/v1/chat/completions
```

This allows dynamic targets without pre-configuration.

## Header Filtering

Both workers filter out hop-by-hop headers to prevent conflicts:

```javascript
connection, keep-alive, proxy-authenticate, proxy-authorization,
te, trailers, transfer-encoding, upgrade,
host, cf-connecting-ip, cf-ray, cf-visitor, cf-ipcountry,
x-forwarded-for, x-forwarded-proto, x-real-ip
```

Additional headers injected by inbound:
- `x-request-id`: Unique request identifier
- `x-bridge-model`: The resolved model name
- `x-bridge-request-id`: Copy of request ID for tracing

## Binary Content Handling

Binary payloads (images, PDFs, etc.) are automatically detected via `Content-Type`:

1. **Inbound**: Binary bodies are base64-encoded before sending to gateway
2. **Outbound**: Binary responses are chunked and base64-encoded as SSE events
3. **Return to Client**: Base64 chunks are decoded back to raw bytes

Content types matching `/text|html|script|xml|json|pdf/i` are treated as text.

## Error Handling

- **Missing config**: 500 status with descriptive message
- **Gateway unreachable**: 502 status
- **Unauthorized outbound**: 500 with SSE error event
- **Target unreachable**: 500 with SSE error event
- **Malformed requests**: Graceful degradation with synthetic init event

Errors always emit a synthetic `init` event first, ensuring the inbound worker never hangs waiting for initialization.

## Backpressure & Streaming

Both workers respect Node.js/Cloudflare streaming backpressure:

- **Inbound**: `ReadableStream.pull()` only processes SSE events when the response body has capacity
- **Outbound**: Target response is read only when the SSE controller has buffer space

This prevents memory bloat on slow clients or targets.

## Usage Example

### Setup Environment

```bash
# Worker Inbound
wrangler env:production secret put AI_GATEWAY_URL https://gateway.example.com/inbound
wrangler env:production secret put AI_GATEWAY_TOKEN my-gateway-token
wrangler env:production secret put OUTBOUND_API_KEY my-outbound-key
wrangler env:production secret put DEFAULT_MODEL gpt-4

# Worker Outbound
wrangler env:production secret put CF_AIG_AUTHORIZATION my-gateway-token
wrangler env:production secret put OUTBOUND_API_KEY my-outbound-key
wrangler env:production secret put ROUTES '{"openai":"https://api.openai.com","anthropic":"https://api.anthropic.com"}'
```

### Make a Request

```bash
curl -X POST https://inbound-worker.example.com/openai/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}'
```

The response streams back in real-time.

## Performance Considerations

- **Streaming**: All bodies are streamed, not buffered. Large files pass through without loading into memory.
- **SSE overhead**: Adds minimal overhead (~50 bytes per chunk for event framing).
- **Base64 encoding**: ~33% size increase for binary payloads (inherent to base64).
- **Backpressure**: Automatically respects client and target speeds—no buffering.

## Limitations & Notes

- **Status codes 101, 204, 205, 304**: Normalized to 200 by outbound worker (no body expected)
- **Content-Type inference**: Falls back to `text/html` for missing or `text/plain` content types
- **Request ID tracking**: Unique per request, useful for distributed tracing
- **Timeout**: Subject to Cloudflare Workers' 30-second limit

## Development

Deploy both workers to your Cloudflare account:

```bash
wrangler publish worker-inbound.js --env production
wrangler publish worker-outbound.js --env production
```

Configure your AI Gateway to forward requests to the outbound worker's URL.

## License

Unlicensed (modify as needed for your use case).
