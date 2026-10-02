/**
 * 🔀 blitz-relay-proxy — PROXY SERVER itu sendiri (bukan storage/rotasi proxy orang)
 *
 *  Deploy di blitz.cloud, dapat URL mis. https://xxx.dannd.blitz.cloud,
 *  lalu PAKAI URL itu sebagai proxy di browser / Puppeteer / curl / Python:
 *
 *    curl -x https://xxx.dannd.blitz.cloud https://api.ipify.org
 *
 *  Cara kerja: forward proxy HTTP (absolute-URI) + CONNECT (HTTPS tunnel)
 *  + relay universal (/api/fetch, /proxy/, /r) + WebSocket upgrade —
 *  semuanya dial LANGSUNG ke target (net.connect / http.request),
 *  TANPA upstream proxy lain. IP yang dilihat target = IP egress blitz.cloud.
 *
 *  Universal: semua method, semua header, body streaming (tanpa buffering),
 *  long request / SSE / file besar OK (requestTimeout = 0).
 *  Tanpa auth secara default; isi PROXY_USER + PROXY_PASS untuk mengunci.
 */
"use strict";

const http = require("http");
const https = require("https");
const net = require("net");
const { URL } = require("url");

try {
  require("dotenv").config();
} catch { /* dotenv opsional */ }

const PORT = parseInt(process.env.PORT || "3000", 10);
const HOST = process.env.HOST || "0.0.0.0";
const PROXY_USER = (process.env.PROXY_USER || "").trim();
const PROXY_PASS = (process.env.PROXY_PASS || "").trim();
const AUTH_REQUIRED = !!(PROXY_USER && PROXY_PASS);

const stats = {
  startedAt: Date.now(),
  forwardHttp: 0,
  connect: 0,
  relay: 0,
  ws: 0,
  bytesUp: 0,
  bytesDown: 0,
};

// ---------------------------------------------------------------- auth
function checkProxyAuth(req) {
  if (!AUTH_REQUIRED) return true;
  const h = req.headers["proxy-authorization"] || req.headers["x-proxy-auth"] || "";
  if (!h) return false;
  let decoded = "";
  if (/^basic\s+/i.test(h)) {
    try {
      decoded = Buffer.from(h.replace(/^basic\s+/i, ""), "base64").toString("utf8");
    } catch { return false; }
  } else {
    decoded = String(h);
  }
  const sep = decoded.indexOf(":");
  const u = sep >= 0 ? decoded.slice(0, sep) : decoded;
  const p = sep >= 0 ? decoded.slice(sep + 1) : "";
  return u === PROXY_USER && p === PROXY_PASS;
}

function denyProxy(res, isConnect, clientSocket) {
  if (isConnect && clientSocket) {
    try { clientSocket.write("HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"relay\"\r\n\r\n"); } catch {}
    try { clientSocket.destroy(); } catch {}
    return;
  }
  sendJson(res, 407, { ok: false, error: "proxy authentication required" }, { "proxy-authenticate": 'Basic realm="relay"' });
}

// ---------------------------------------------------------------- helpers
function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "*",
    "access-control-allow-headers": "*",
    "access-control-expose-headers": "*",
  };
}

function sendJson(res, code, obj, extra = {}) {
  const body = Buffer.from(JSON.stringify(obj));
  res.writeHead(code, {
    "content-type": "application/json; charset=utf-8",
    "content-length": body.length,
    ...corsHeaders(),
    ...extra,
  });
  res.end(body);
}

const DROP_REQ = new Set([
  "host", "connection", "proxy-connection", "proxy-authorization",
  "proxy-authenticate", "keep-alive", "transfer-encoding", "upgrade",
  "x-proxy-auth", "x-target-url", "x-target-method",
  "x-relay-target", "x-relay-path", // 9router relay protocol (jangan diteruskan ke target)
]);
const DROP_RES = new Set([
  "connection", "proxy-connection", "proxy-authenticate",
  "keep-alive", "transfer-encoding", "upgrade",
]);

function buildUpstreamHeaders(clientHeaders, overrides) {
  const out = {};
  for (const [k, v] of Object.entries(clientHeaders || {})) {
    const lk = k.toLowerCase();
    if (DROP_REQ.has(lk)) continue;
    out[lk] = v;
  }
  if (overrides) {
    for (const [k, v] of Object.entries(overrides)) {
      const lk = k.toLowerCase();
      if (DROP_REQ.has(lk)) continue;
      if (v === undefined || v === null) delete out[lk];
      else out[lk] = v;
    }
  }
  delete out["host"];
  delete out["content-length"]; // streaming chunked kecuali spec memaksa
  return out;
}

// ---------------------------------------------------------------- core relay (streaming, tanpa buffering)
function relayDirect(clientReq, clientRes, targetUrlStr, opts = {}) {
  let target;
  try {
    target = new URL(targetUrlStr);
  } catch {
    sendJson(clientRes, 400, { ok: false, error: "invalid target url" });
    return;
  }
  if (!["http:", "https:"].includes(target.protocol)) {
    sendJson(clientRes, 400, { ok: false, error: "only http/https target supported" });
    return;
  }

  const method = (opts.method || clientReq.method || "GET").toUpperCase();
  const isHttps = target.protocol === "https:";
  const lib = isHttps ? https : http;
  const headers = buildUpstreamHeaders(clientReq.headers, opts.headers);
  if (opts.forceHeaders) {
    for (const [k, v] of Object.entries(opts.forceHeaders)) headers[k.toLowerCase()] = v;
  }

  const proxyReq = lib.request(target, { method, headers }, (proxyRes) => {
    const outH = {};
    for (const [k, v] of Object.entries(proxyRes.headers || {})) {
      if (DROP_RES.has(k.toLowerCase())) continue;
      outH[k] = v;
    }
    Object.assign(outH, corsHeaders());
    // NOTE: jangan set x-relay-target di response — 9router tidak mengharapkannya
    // dan worker resmi juga tidak mengirimnya balik. Hindari bentrok header.
    outH["x-accel-buffering"] = "no";
    if (!outH["cache-control"]) outH["cache-control"] = "no-cache";

    clientRes.writeHead(proxyRes.statusCode || 502, outH);
    stats.relay++;
    proxyRes.on("data", (c) => { stats.bytesDown += c.length; });
    proxyRes.pipe(clientRes);
    proxyRes.on("error", () => { try { clientRes.destroy(); } catch {} });
  });

  proxyReq.on("error", (e) => {
    if (!clientRes.headersSent) {
      sendJson(clientRes, 502, {
        ok: false,
        error: String((e && e.message) || e),
        target: `${target.protocol}//${target.host}${target.pathname}`,
      });
    } else {
      try { clientRes.destroy(); } catch {}
    }
  });

  // TANPA timeout upstream — long request / SSE / stream dibiarkan hidup.
  // blitz / reverse-proxy di depan yang mengatur batasnya.

  clientRes.on("close", () => {
    try { if (!proxyReq.destroyed) proxyReq.destroy(); } catch {}
  });

  if (opts.specBody !== undefined && opts.specBody !== null) {
    let buf;
    if (Buffer.isBuffer(opts.specBody)) buf = opts.specBody;
    else if (typeof opts.specBody === "object") buf = Buffer.from(JSON.stringify(opts.specBody));
    else buf = Buffer.from(String(opts.specBody));
    stats.bytesUp += buf.length;
    proxyReq.write(buf);
    proxyReq.end();
    try { clientReq.resume(); } catch {}
  } else {
    clientReq.on("data", (c) => { stats.bytesUp += c.length; });
    clientReq.on("error", () => { try { proxyReq.destroy(); } catch {} });
    clientReq.pipe(proxyReq);
  }
}

function directConnect(host, port, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const sock = net.connect(port, host);
    const timer = setTimeout(() => { sock.destroy(); reject(new Error("connect timeout")); }, timeoutMs);
    sock.once("connect", () => { clearTimeout(timer); resolve(sock); });
    sock.once("error", (e) => { clearTimeout(timer); reject(e); });
  });
}

function readJsonBody(req, limit = 5 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (c) => {
      size += c.length;
      if (size > limit) {
        reject(new Error("json body too large"));
        try { req.destroy(); } catch {}
      } else chunks.push(c);
    });
    req.on("end", () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"));
      } catch {
        reject(new Error("invalid json body"));
      }
    });
    req.on("error", reject);
  });
}

// ---------------------------------------------------------------- dashboard
function dashboardHtml(host) {
  const up = Math.floor((Date.now() - stats.startedAt) / 1000);
  const authLine = AUTH_REQUIRED
    ? `🔒 Auth: <b>AKTIF</b> (user <code>${PROXY_USER}</code>). Client wajib kirim <code>Proxy-Authorization: Basic base64(user:pass)</code>.`
    : `🔓 Auth: <b>TERBUKA</b> (tanpa login). Kunci dengan env <code>PROXY_USER</code> + <code>PROXY_PASS</code>.`;
  return `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Proxy Server — blitz.cloud</title>
<style>body{font-family:system-ui,sans-serif;max-width:820px;margin:32px auto;padding:0 16px;line-height:1.65}
code,pre{background:#f4f4f5;padding:2px 6px;border-radius:6px}pre{padding:12px;overflow:auto}
.card{border:1px solid #e4e4e7;border-radius:12px;padding:16px;margin:12px 0}
h1{margin-bottom:4px}.muted{color:#71717a}</style></head><body>
<h1>🔀 Proxy Server ONLINE</h1>
<p>Uptime ${up}s · Forward-HTTP: ${stats.forwardHttp} · CONNECT: ${stats.connect} · Relay: ${stats.relay} · WS: ${stats.ws}</p>
<p>${authLine}</p>
<div class="card"><h3>Pakai sebagai PROXY (cara utama — server ini proxy-nya)</h3>
<pre>http://${host}</pre>
<p>
curl: <code>curl -x http://${host} https://api.ipify.org</code><br>
Puppeteer: <code>args: ["--proxy-server=http://${host}"]</code><br>
Python: <code>proxies = {"http": "http://${host}", "https": "http://${host}"}</code><br>
Browser / OS: isi HTTP proxy = host ini, port 80/443.</p>
${AUTH_REQUIRED ? `<p>Dengan auth:<br><code>curl -x http://${PROXY_USER}:${PROXY_PASS}@${host} https://api.ipify.org</code></p>` : ``}</div>
<div class="card"><h3>Kompatibel 9router proxy pool (type: vercel)</h3>
<pre>proxyUrl = https://${host}   (type: vercel)</pre>
<p>9router mengirim <code>x-relay-target: https://target-host</code> + <code>x-relay-path: /path</code> ke path apa pun (termasuk <code>/</code>) — diteruskan 1:1, response dibalikkan mentah. Tambahkan di dashboard 9router → Proxy Pools → Add.</p></div>
<div class="card"><h3>Tanpa setting proxy (relay universal — method/header/body diteruskan mentah, streaming)</h3>
<pre>GET  /api/fetch?url=https://api.ipify.org
GET  /r?url=https://api.ipify.org
GET  /proxy/https://api.ipify.org
POST /api/fetch?url=https://httpbin.org/post   (body streaming)
POST /api/fetch  {"url":"https://...","method":"POST","headers":{},"body":{...}}</pre></div>
<div class="card"><h3>Manajemen</h3>
<ul><li><code>GET /health</code> · <code>GET /api/stats</code></li></ul></div>
<p class="muted">IP yang dilihat target = IP egress blitz.cloud · semua method · semua header · long request · SSE · WebSocket · CORS *</p>
</body></html>`;
}

// ---------------------------------------------------------------- router
async function handleRequest(req, res) {
  const rawUrl = req.url || "/";

  // 1) MODE FORWARD PROXY: request-line absolut (dipakai otomatis saat client set proxy)
  if (/^https?:\/\//i.test(rawUrl)) {
    if (!checkProxyAuth(req)) {
      denyProxy(res, false);
      return;
    }
    stats.forwardHttp++;
    relayDirect(req, res, rawUrl, {});
    return;
  }

  let pathOnly = rawUrl;
  let queryStr = "";
  const qi = rawUrl.indexOf("?");
  if (qi >= 0) {
    pathOnly = rawUrl.slice(0, qi);
    queryStr = rawUrl.slice(qi + 1);
  }
  let urlObj;
  try {
    urlObj = new URL(rawUrl, "http://local");
  } catch {
    urlObj = new URL("/", "http://local");
  }

  // CORS preflight global — jawab untuk SEMUA path agar relay 9router
  // (yang bisa ke path apa pun, termasuk "/") lolos dari browser.
  if (req.method === "OPTIONS") {
    res.writeHead(204, { ...corsHeaders(), "access-control-max-age": "86400", "content-length": "0" });
    res.end();
    return;
  }

  // 2) PROTOKOL RELAY 9ROUTER (prioritas — dipakai proxyAwareFetch):
  //   fetch(poolUrl, { headers: {
  //     "x-relay-target": "https://target-host",   // WAJIB: scheme + host (+port)
  //     "x-relay-path": "/path?q=1",               // opsional, default "/"
  //     ...semua header asli client
  //   }})
  //   -> method + headers (minus x-relay-*) + body diteruskan 1:1 (streaming),
  //      response target dibalikkan mentah (status + headers + body).
  //   Berlaku untuk SEMUA path (root "/" juga), agar kompatibel dengan
  //   worker relay resmi 9router (cloudflare/vercel/deno deploy).
  //   HARUS sebelum handler dashboard "/" — 9router menembak root "/" + header ini.
  const relayTarget = req.headers["x-relay-target"];
  if (relayTarget) {
    if (!checkProxyAuth(req)) {
      denyProxy(res, false);
      return;
    }
    const relayPath = req.headers["x-relay-path"] || "/";
    const base = String(relayTarget).replace(/\/$/, "");
    const target = base + (String(relayPath).startsWith("/") ? String(relayPath) : "/" + String(relayPath));
    relayDirect(req, res, target, { method: req.method });
    return;
  }

  if (req.method === "GET" && pathOnly === "/") {
    const body = dashboardHtml(req.headers.host || `localhost:${PORT}`);
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "content-length": Buffer.byteLength(body), ...corsHeaders() });
    res.end(body);
    return;
  }

  if (req.method === "GET" && pathOnly === "/health") {
    sendJson(res, 200, {
      ok: true,
      proxy: true,
      auth: AUTH_REQUIRED ? "required" : "open",
      uptimeSec: Math.floor((Date.now() - stats.startedAt) / 1000),
      forwardHttp: stats.forwardHttp,
      connect: stats.connect,
      relay: stats.relay,
    });
    return;
  }
  if (req.method === "GET" && pathOnly === "/api/stats") {
    sendJson(res, 200, {
      ok: true,
      auth: AUTH_REQUIRED ? "required" : "open",
      uptimeSec: Math.floor((Date.now() - stats.startedAt) / 1000),
      totals: {
        forwardHttp: stats.forwardHttp,
        connect: stats.connect,
        relay: stats.relay,
        ws: stats.ws,
        bytesUp: stats.bytesUp,
        bytesDown: stats.bytesDown,
      },
    });
    return;
  }

  // 3) RELAY UNIVERSAL (untuk client yang tidak bisa set proxy)
  if (pathOnly === "/api/fetch" || pathOnly === "/r" || pathOnly === "/api/relay") {
    const ct = (req.headers["content-type"] || "").toLowerCase();
    const qUrl = urlObj.searchParams.get("url") || req.headers["x-target-url"];
    if (qUrl) {
      let target = String(qUrl);
      const extra = [];
      for (const [k, v] of urlObj.searchParams.entries()) {
        if (k === "url") continue;
        extra.push(`${encodeURIComponent(k)}=${encodeURIComponent(v)}`);
      }
      if (extra.length) target += (target.includes("?") ? "&" : "?") + extra.join("&");
      const method = req.headers["x-target-method"] || req.method;
      relayDirect(req, res, target, { method });
      return;
    }
    if (ct.includes("application/json")) {
      let b;
      try {
        b = await readJsonBody(req);
      } catch (e) {
        sendJson(res, 400, { ok: false, error: String(e.message || e) });
        return;
      }
      if (!b.url && !b.target) {
        sendJson(res, 400, { ok: false, error: 'missing "url". Contoh: /api/fetch?url=https://api.ipify.org' });
        return;
      }
      const target = String(b.url || b.target);
      const forceHeaders = {};
      if (b.headers && typeof b.headers === "object") Object.assign(forceHeaders, b.headers);
      let specBody = b.body !== undefined ? b.body : null;
      if (specBody !== null && specBody !== undefined) {
        const hasCT = Object.keys(forceHeaders).some((k) => k.toLowerCase() === "content-type");
        if (!hasCT) forceHeaders["content-type"] = "application/json";
      }
      relayDirect(req, res, target, {
        method: b.method || "GET",
        headers: {},
        forceHeaders,
        specBody,
      });
      return;
    }
    sendJson(res, 400, { ok: false, error: "missing ?url=. Contoh: /api/fetch?url=https://api.ipify.org" });
    return;
  }

  if (pathOnly === "/proxy" || pathOnly.startsWith("/proxy/")) {
    let rest = pathOnly.slice("/proxy".length);
    if (rest.startsWith("/")) rest = rest.slice(1);
    try {
      rest = decodeURIComponent(rest);
    } catch { /* biarkan mentah */ }
    let target = rest;
    if (queryStr) target += "?" + queryStr;
    if (!target || !/^https?:\/\//i.test(target)) {
      sendJson(res, 400, { ok: false, error: "usage: /proxy/https://target-host/path?q=1" });
      return;
    }
    relayDirect(req, res, target, { method: req.headers["x-target-method"] || req.method });
    return;
  }

  sendJson(res, 404, {
    ok: false,
    error: "not found",
    hint: "Set proxy client ke http://host-ini, atau GET /api/fetch?url=https://... ",
  });
}

// ---------------------------------------------------------------- server
const server = http.createServer((req, res) => {
  handleRequest(req, res).catch((e) => {
    if (!res.headersSent) sendJson(res, 500, { ok: false, error: String((e && e.message) || e) });
    else try { res.destroy(); } catch {}
  });
});

// Long request / SSE / file besar: jangan putus koneksi.
server.requestTimeout = 0;
server.headersTimeout = 120000;
server.keepAliveTimeout = 65000;
server.maxHeadersCount = 100;

// CONNECT — HTTPS tunnel, dial LANGSUNG ke target (server ini proxy-nya)
server.on("connect", async (req, clientSocket, head) => {
  if (!checkProxyAuth(req)) {
    denyProxy(null, true, clientSocket);
    return;
  }
  stats.connect++;
  const authority = req.url || "";
  const sep = authority.lastIndexOf(":");
  const targetHost = sep > 0 ? authority.slice(0, sep).replace(/^\[|\]$/g, "") : authority;
  const targetPort = sep > 0 ? parseInt(authority.slice(sep + 1), 10) : 443;
  if (!targetHost || !targetPort) {
    try { clientSocket.write("HTTP/1.1 400 Bad Request\r\n\r\n"); } catch {}
    return clientSocket.destroy();
  }
  try {
    const upstream = await directConnect(targetHost, targetPort);
    clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
    clientSocket.setNoDelay(true);
    clientSocket.setTimeout(0);
    upstream.setNoDelay(true);
    upstream.setTimeout(0);
    if (head && head.length) upstream.write(head);
    clientSocket.pipe(upstream);
    upstream.pipe(clientSocket);
    const bye = () => {
      try { clientSocket.destroy(); } catch {}
      try { upstream.destroy(); } catch {}
    };
    clientSocket.on("error", bye);
    upstream.on("error", bye);
  } catch {
    try { clientSocket.write("HTTP/1.1 502 Bad Gateway\r\n\r\n"); } catch {}
    try { clientSocket.destroy(); } catch {}
  }
});

// WebSocket / Upgrade — handshake diteruskan mentah ke target
server.on("upgrade", async (req, clientSocket, head) => {
  try {
    let targetStr = null;
    if (/^wss?:\/\//i.test(req.url || "")) targetStr = req.url;
    else if ((req.url || "").startsWith("/proxy/")) {
      let rest = req.url.slice("/proxy/".length).split("?")[0];
      try { rest = decodeURIComponent(rest); } catch {}
      if (/^(wss?|https?):\/\//i.test(rest)) targetStr = rest.replace(/^ws/i, "http");
    } else if (req.url.startsWith("/api/fetch") || req.url.startsWith("/r")) {
      const u = new URL(req.url, "http://local");
      const t = u.searchParams.get("url") || req.headers["x-target-url"];
      if (t) targetStr = String(t).replace(/^ws/i, "http");
    }
    if (!targetStr) {
      try { clientSocket.write("HTTP/1.1 400 Bad Request\r\n\r\n"); } catch {}
      return clientSocket.destroy();
    }
    const t = new URL(targetStr);
    const targetPort = t.port ? parseInt(t.port, 10) : t.protocol.startsWith("https") || t.protocol.startsWith("wss") ? 443 : 80;
    stats.ws++;
    const upstream = await directConnect(t.hostname, targetPort);

    const fwdH = {};
    for (const [k, v] of Object.entries(req.headers || {})) {
      const lk = k.toLowerCase();
      if (["x-target-url", "proxy-authorization", "x-proxy-auth"].includes(lk)) continue;
      fwdH[k] = v;
    }
    fwdH["Host"] = t.host;
    let raw = `${req.method} ${t.pathname}${t.search} HTTP/1.1\r\n`;
    for (const [k, v] of Object.entries(fwdH)) raw += `${k}: ${v}\r\n`;
    raw += `\r\n`;
    upstream.write(raw);
    if (head && head.length) upstream.write(head);
    clientSocket.pipe(upstream);
    upstream.pipe(clientSocket);
    const bye = () => { try { clientSocket.destroy(); } catch {} try { upstream.destroy(); } catch {} };
    clientSocket.on("error", bye);
    upstream.on("error", bye);
  } catch {
    try { clientSocket.write("HTTP/1.1 502 Bad Gateway\r\n\r\n"); } catch {}
    try { clientSocket.destroy(); } catch {}
  }
});

server.listen(PORT, HOST, () => {
  console.log(`[proxy] server ini ADALAH proxy — listening on ${HOST}:${PORT}`);
  console.log(`[proxy] auth: ${AUTH_REQUIRED ? `required (user ${PROXY_USER})` : "open (tanpa login)"}`);
});

module.exports = { server };
