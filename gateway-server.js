import {
  env
} from "cloudflare:workers";

const {
  PROVIDER_KEY
} = env;
const gatewayHost = /gateway\.ai\.cloudflare\.com/gi;
const gatewayPrefix = /^https:\/\/gateway\.ai\.cloudflare\.com\/v1\/[^\/]+\/[^\/]+\/[^\/]+\//gi;

const normalizeRequest = (request) => {
  const url = new URL(request.url);
  url.pathname = (url.pathname || '').replace(/^\/v1/, '');
  const reqHost = RegExp(String(request.headers.get('x-gateway-source')), 'gi');
  const reqHeaders = new Headers(request.headers);
  reqHeaders.delete('x-gateway-source');

  for (const [key, value] of reqHeaders) {
    reqHeaders.set(key, value
      .replaceAll(gatewayPrefix, url.origin + '/')
      .replaceAll(gatewayHost, url.host)
      .replaceAll(reqHost, url.host)
    );
  }
  const requestInit = {
    method: request.method,
    headers: reqHeaders,
  };
  if (request.body) {
    requestInit.body = request.body;
  }
  return new Request(String(url), requestInit);
};

const gatewayServer = async (request) => {
  if (request.headers.get('x-provider-key') !== PROVIDER_KEY) {
    return new Response(null, {
      status: 403
    });
  }
  request = normalizeRequest(request);

  //do request logic

  return new Response("my response", {
    headers: {
      "x-gateway-target": new URL(request.url).host
    }
  });
};

export default {
  async fetch(request) {
    return gatewayServer(request);
  },
};
