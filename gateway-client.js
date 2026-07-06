import {
  env
} from "cloudflare:workers";

const {
  ACCOUNT_ID,
  AI_GATEWAY_ID,
  CF_AIG_AUTHORIZATION,
  AI_PROVIDER,
  PROVIDER_KEY
} = env;

const gatewayHost = /gateway\.ai\.cloudflare\.com/gi;
const gatewayPrefix = /^https:\/\/gateway\.ai\.cloudflare\.com\/v1\/[^\/]+\/[^\/]+\/[^\/]+\//gi;

const fetchResponse = async (...args) => {
  try {
    return await fetch(...args);
  } catch (e) {
    return new Response(String(e), {
      status: 500,
      statusText: String(e)
    });
  }
};

const gatewayClient = async (request) => {
  const url = new URL(request.url);
  const reqHost = url.host;
  const path = url.pathname.replace(/^\/+/, '');
  const gatewayUrl =
    `https://gateway.ai.cloudflare.com/v1/${ACCOUNT_ID}/${AI_GATEWAY_ID}/custom-${AI_PROVIDER}/${path}${url.search}`;
  console.log(gatewayUrl)
  const reqHeaders = new Headers(request.headers);
  reqHeaders.set('cf-aig-authorization', `Bearer ${CF_AIG_AUTHORIZATION}`);
  reqHeaders.set('x-gateway-source', reqHost);
  reqHeaders.set('x-provider-key', PROVIDER_KEY);

  const requestInit = {
    method: request.method,
    headers: reqHeaders,
  };

  if (request.body) {
    requestInit.body = request.body;
  }

  const res = await fetchResponse(gatewayUrl, requestInit);
  const remoteHost = RegExp(String(res.headers.get('x-gateway-target')), "gi");
  const resHeaders = new Headers(res.headers.entries());
  resHeaders.delete('x-gateway-target');

  for (const [key, value] of resHeaders) {
    resHeaders.set(key, value
      .replaceAll(remoteHost, reqHost)
      .replaceAll(gatewayPrefix, `https://${reqHost}/`)
      .replaceAll(gatewayHost, reqHost)
    );
  }

  return new Response(res.body, {
    status: res.status,
    statusText: res.statusText,
    headers: resHeaders
  });
};

export default {
  async fetch(request) {
    return gatewayClient(request);
  },
};
