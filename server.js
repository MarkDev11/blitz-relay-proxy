/**
 * 🔀 Universal Relay / Forward Proxy — blitz.cloud ready
 *
 *  - Forward proxy (HTTP absolute-URI + CONNECT) di PORT yang sama
 *  - Universal HTTP relay: semua method, semua header, streaming body,
 *    support SSE / long-lived / file besar (TANPA buffering body)
 *  - Pool upstream dengan rotasi roundrobin/random + cooldown + sticky ?proxy=
 *  - Format proxy: protocol://user:pass@host:port  ATAU  host:port:user:pass
 *  - TANPA AUTH, CORS terbuka
 *
 *  Env:
 *    PORT, HOST, PROXIES, PROXY_FILE, ROTATE=roundrobin|random,
 *    UPSTREAM_TIMEOUT_MS=0 (0 = tanpa timeout, untuk long request),
 *    CONNECT_TIMEOUT_MS=20000, CHECK_URL, PROXY_COOLDOWN_MS, PROXY_MAX_FAILS
 */
"use strict";

const http = require("http");
const https = require("https");
const net = require("net");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

try {
  require("dotenv").config();
} catch { /* dotenv opsional */ }

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT || "3000", 10);
const HOST = process.env.HOST || "0.0.0.0";
const PROXY_FILE =
  process.env.PROXY_FILE ||
  (fs.existsSync(path.join(__dirname, "proxies.txt"))
    ? path.join(__dirname, "proxies.txt")
    : null);
const STRATEGY = (process.env.ROTATE || "roundrobin").toLowerCase();
const UPSTREAM_TIMEOUT_MS = parseInt(process.env.UPSTREAM_TIMEOUT_MS || process.env.FETCH_TIMEOUT_MS || "0", 10); // 0 = no timeout
const CONNECT_TIMEOUT_MS = parseInt(process.env.CONNECT_TIMEOUT_MS || "20000", 10);
const DEFAULT_CHECK_URL = process.env.CHECK_URL || "http://httpbin.org/ip";
const COOLDOWN_MS = parseInt(process.env.PROXY_COOLDOWN_MS || "60000", 10);
const MAX_FAILS = parseInt(process.env.PROXY_MAX_FAILS || "3", 10);
const JSON_SPEC_LIMIT = 5 * 1024 * 1024; // hanya untuk mode JSON-spec, bukan relay body

// ---------------------------------------------------------------------------
// Proxy line parsing
//   protocol://user:pass@host:port | protocol://host:port
//   user:pass@host:port | host:port:user:pass | host:port
// ---------------------------------------------------------------------------
function parseProxyLine(raw) {
  const line = String(raw || "").trim();
  if (!line || line.startsWith("#")) return null;

  if (line.includes("://")) {
    try {
      const u = new URL(line);
      const protocol = u.protocol.replace(":", "").toLowerCase();
      if (!["http", "https", "socks", "socks4", "socks5", "socks5h"].includes(protocol)) {
        return { error: `unsupported protocol: ${protocol}`, raw: line };
      }
      const DEFAULT_PORTS = { http: 80, https: 443, socks: 1080, socks4: 1080, socks5: 1080, socks5h: 1080 };
      const port = u.port ? parseInt(u.port, 10) : DEFAULT_PORTS[protocol] || 0;
      const host = u.hostname;
      if (!host || !port) return { error: "missing host/port", raw: line };
      return {
        raw: line,
        protocol: protocol === "socks" ? "socks5" : protocol,
        host,
        port,
        user: decodeURIComponent(u.username || ""),
        pass: decodeURIComponent(u.password || ""),
      };
    } catch {
      return { error: "invalid URL format", raw: line };
    }
  }

  if (line.includes("@")) {
    try {
      const u = new URL("http://" + line);
      const host = u.hostname;
      const port = parseInt(u.port, 10);
      if (!host || !port) return { error: "missing host/port", raw: line };
      return {
        raw: line,
        protocol: "http",
        host,
        port,
        user: decodeURIComponent(u.username || ""),
        pass: decodeURIComponent(u.password || ""),
      };
    } catch {
      return { error: "invalid user:pass@host:port format", raw: line };
    }
  }

  const parts = line.split(":");
  if (parts.length === 4) {
    const [host, portStr, user, pass] = parts;
    const port = parseInt(portStr, 10);
    if (!host || !port) return { error: "invalid host:port:user:pass", raw: line };
    return { raw: line, protocol: "http", host, port, user, pass };
  }
  if (parts.length === 2) {
    const [host, portStr] = parts;
    const port = parseInt(portStr, 10);
    if (!host || !port) return { error: "invalid host:port", raw: line };
    return { raw: line, protocol: "http", host, port, user: "", pass: "" };
  }
  // IPv6 [::1]:port  atau host aneh -> coba parse manual "host:port" terakhir
  const m = line.match(/^(.*):(\d+)$/);
  if (m) {
    const port = parseInt(m[2], 10);
    if (m[1] && port) return { raw: line, protocol: "http", host: m[1].replace(/^\[|\]$/g, ""), port, user: "", pass: "" };
  }
  return { error: "unknown format. Use protocol://user:pass@host:port or host:port:user:pass", raw: line };
}

function maskProxy(p) {
  if (!p) return "direct";
  const auth = p.user ? `${p.user}:***@` : "";
  return `${p.protocol}://${auth}${p.host}:${p.port}`;
}
function toProxyUrl(p) {
  const auth = p.user ? `${encodeURIComponent(p.user)}:${encodeURIComponent(p.pass)}@` : "";
  return `${p.protocol}://${auth}${p.host}:${p.port}`;
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------
function loadPool() {
  const lines = [];
  if (process.env.PROXIES) lines.push(...String(process.env.PROXIES).split(/[\n,;]+/));
  const file = process.env.PROXY_FILE || PROXY_FILE;
  if (file && fs.existsSync(file)) {
    lines.push(...fs.readFileSync(file, "utf-8").split(/\r?\n/));
  }
  const pool = [];
  const errors = [];
  for (const l of lines) {
    const t = String(l || "").trim();
    if (!t || t.startsWith("#")) continue;
    const parsed = parseProxyLine(t);
    if (!parsed) continue;
    if (parsed.error) errors.push(parsed);
    else {
      parsed.fails = 0;
      parsed.cooldownUntil = 0;
      parsed.uses = 0;
      pool.push(parsed);
    }
  }
  return { pool, errors };
}

let { pool: POOL, errors: LOAD_ERRORS } = loadPool();
let rrIndex = 0;

const stats = {
  startedAt: Date.now(),
  totalRelay: 0,
  totalConnect: 0,
  totalBytesUp: 0,
  totalBytesDown: 0,
  proxyUses: {},
  proxyFails: {},
};

function poolAlive() {
  const now = Date.now();
  return POOL.filter((p) => p.cooldownUntil <= now);
}

function pickProxy(preferredRaw) {
  if (preferredRaw !== null && preferredRaw !== undefined && String(preferredRaw).trim() !== "") {
    const s = String(preferredRaw).trim();
    const parsed = parseProxyLine(s);
    if (parsed && !parsed.error) {
      parsed._adhoc = true;
      return parsed;
    }
    if (/^\d+$/.test(s)) {
      const idx = parseInt(s, 10);
      if (POOL[idx]) return POOL[idx];
    }
    // format salah -> jangan error, fallback ke rotasi (tapi tandai)
  }
  const alive = poolAlive();
  const source = alive.length > 0 ? alive : POOL;
  if (source.length === 0) return null; // mode direct
  let p;
  if (STRATEGY === "random") {
    p = source[Math.floor(Math.random() * source.length)];
  } else {
    p = source[rrIndex % source.length];
    rrIndex = (rrIndex + 1) % 1000000;
  }
  p.uses++;
  const key = `${p.host}:${p.port}`;
  stats.proxyUses[key] = (stats.proxyUses[key] || 0) + 1;
  return p;
}

function markFail(p) {
  if (!p || p._adhoc) return;
  p.fails = (p.fails || 0) + 1;
  const key = `${p.host}:${p.port}`;
  stats.proxyFails[key] = (stats.proxyFails[key] || 0) + 1;
  if (p.fails >= MAX_FAILS) {
    p.cooldownUntil = Date.now() + COOLDOWN_MS;
    p.fails = 0;
  }
}
function markOk(p) {
  if (!p || p._adhoc) return;
  p.fails = 0;
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------
const { HttpProxyAgent } = require("http-proxy-agent");
const { HttpsProxyAgent } = require("https-proxy-agent");
const { SocksProxyAgent } = require("socks-proxy-agent");

function agentFor(proxy, targetIsHttps) {
  if (!proxy) return undefined;
  const url = toProxyUrl(proxy);
  if (proxy.protocol.startsWith("socks")) return new SocksProxyAgent(url);
  return targetIsHttps ? new HttpsProxyAgent(url) : new HttpProxyAgent(url);
}

// ---------------------------------------------------------------------------
// Header helpers — universal: teruskan SEMUA kecuali hop-by-hop & kontrol
// ---------------------------------------------------------------------------
const DROP_REQ = new Set([
  "host", "connection", "proxy-connection", "proxy-authorization",
  "proxy-authenticate", "keep-alive", "transfer-encoding", "upgrade",
  "x-proxy", "x-upstream-proxy", "x-target-url", "x-target-method",
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
  // host dihitung ulang oleh node dari target URL
  delete out["host"];
  delete out["content-length"]; // biarkan chunked/streaming kecuali spec menegaskan
  return out;
}

function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "*",
    "access-control-allow-headers": "*",
    "access-control-expose-headers": "*",
  };
}

// ---------------------------------------------------------------------------
// Core: streaming relay — TANPA buffering body
// ---------------------------------------------------------------------------
function relayStream(clientReq, clientRes, targetUrlStr, opts = {}) {
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

  const proxy = opts.proxy !== undefined ? opts.proxy : pickProxy(opts.preferredProxy);
  const method = (opts.method || clientReq.method || "GET").toUpperCase();
  const isHttps = target.protocol === "https:";
  const lib = isHttps ? https : http;
  const agent = agentFor(proxy, isHttps);

  const headers = buildUpstreamHeaders(clientReq.headers, opts.headers);
  // JSON-spec boleh memaksa content-length/content-type sendiri
  if (opts.forceHeaders) {
    for (const [k, v] of Object.entries(opts.forceHeaders)) headers[k.toLowerCase()] = v;
  }

  const timeout = opts.timeout !== undefined ? opts.timeout : UPSTREAM_TIMEOUT_MS;

  const proxyReq = lib.request(
    target,
    { method, headers, agent },
    (proxyRes) => {
      if (proxy) markOk(proxy);
      // teruskan status + SEMUA header upstream (kecuali hop-by-hop)
      const outH = {};
      for (const [k, v] of Object.entries(proxyRes.headers || {})) {
        if (DROP_RES.has(k.toLowerCase())) continue;
        outH[k] = v;
      }
      Object.assign(outH, corsHeaders());
      outH["x-relay-proxy"] = proxy ? `${proxy.host}:${proxy.port}` : "direct";
      outH["x-relay-target"] = `${target.host}`;
      // SSE / streaming: matikan buffering perantara
      outH["x-accel-buffering"] = "no";
      outH["cache-control"] = outH["cache-control"] || "no-cache";

      clientRes.writeHead(proxyRes.statusCode || 502, outH);
      stats.totalRelay++;
      proxyRes.on("data", (c) => { stats.totalBytesDown += c.length; });
      proxyRes.pipe(clientRes);
      proxyRes.on("error", () => { try { clientRes.destroy(); } catch {} });
    }
  );

  proxyReq.on("error", (e) => {
    if (proxy) markFail(proxy);
    if (!clientRes.headersSent) {
      sendJson(clientRes, 502, {
        ok: false,
        error: String((e && e.message) || e),
        via: maskProxy(proxy),
        target: `${target.protocol}//${target.host}${target.pathname}`,
      });
    } else {
      try { clientRes.destroy(); } catch {}
    }
  });

  if (timeout && timeout > 0) {
    proxyReq.setTimeout(timeout, () => {
      proxyReq.destroy(new Error(`upstream timeout after ${timeout}ms`));
    });
  }

  // client abort -> batalkan upstream (hemat koneksi pool)
  clientRes.on("close", () => {
    try { if (!proxyReq.destroyed) proxyReq.destroy(); } catch {}
  });

  // JSON-spec dengan body kecil (string/objek) -> tulis langsung, SELAIN itu stream
  if (opts.specBody !== undefined && opts.specBody !== null) {
    let buf;
    if (Buffer.isBuffer(opts.specBody)) buf = opts.specBody;
    else if (typeof opts.specBody === "object") buf = Buffer.from(JSON.stringify(opts.specBody));
    else buf = Buffer.from(String(opts.specBody));
    stats.totalBytesUp += buf.length;
    proxyReq.write(buf);
    proxyReq.end();
    try { clientReq.resume(); } catch {} // buang sisa stream client (sudah dibaca readJsonBody)
  } else {
    // STREAMING: pipe langsung, tanpa buffering — support file besar & SSE upload
    clientReq.on("data", (c) => { stats.totalBytesUp += c.length; });
    clientReq.on("error", () => { try { proxyReq.destroy(); } catch {} });
    clientReq.pipe(proxyReq);
  }
}

// ---------------------------------------------------------------------------
// CONNECT chaining (HTTPS / TCP tunnel via upstream)
// ---------------------------------------------------------------------------
function socks5Connect(proxy, targetHost, targetPort, timeout = CONNECT_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const sock = net.connect(proxy.port, proxy.host);
    const timer = setTimeout(() => {
      sock.destroy();
      reject(new Error("socks connect timeout"));
    }, timeout);
    const cleanup = () => clearTimeout(timer);
    sock.on("error", (e) => { cleanup(); reject(e); });
    sock.once("connect", () => {
      const hasAuth = !!(proxy.user || proxy.pass);
      sock.write(hasAuth ? Buffer.from([0x05, 0x02, 0x00, 0x02]) : Buffer.from([0x05, 0x01, 0x00]));
      let stage = 0;
      let buf = Buffer.alloc(0);
      sock.on("data", (chunk) => {
        buf = Buffer.concat([buf, chunk]);
        if (stage === 0) {
          if (buf.length < 2) return;
          if (buf[0] !== 0x05) { cleanup(); sock.destroy(); return reject(new Error("bad socks greeting")); }
          if (buf[1] === 0x02 && hasAuth) {
            const u = Buffer.from(proxy.user || "");
            const pw = Buffer.from(proxy.pass || "");
            sock.write(Buffer.concat([Buffer.from([0x01, u.length]), u, Buffer.from([pw.length]), pw]));
            buf = Buffer.alloc(0);
            stage = 0.5;
            return;
          }
          if (buf[1] !== 0x00) { cleanup(); sock.destroy(); return reject(new Error("socks auth required but no credentials")); }
          buf = Buffer.alloc(0);
          stage = 1;
          sendConnect();
        } else if (stage === 0.5) {
          if (buf.length < 2) return;
          if (buf[1] !== 0x00) { cleanup(); sock.destroy(); return reject(new Error("socks user/pass auth failed")); }
          buf = Buffer.alloc(0);
          stage = 1;
          sendConnect();
        } else if (stage === 1) {
          if (buf.length < 4) return;
          if (buf[1] !== 0x00) { cleanup(); sock.destroy(); return reject(new Error("socks connect failed code=" + buf[1])); }
          const atyp = buf[3];
          const need = atyp === 1 ? 10 : atyp === 3 ? 5 + buf[4] + 2 : 22;
          if (buf.length < need) return;
          cleanup();
          sock.removeAllListeners("data");
          resolve(sock);
        }
      });
      function sendConnect() {
        const hostBuf = Buffer.from(targetHost);
        sock.write(Buffer.concat([
          Buffer.from([0x05, 0x01, 0x00, 0x03, hostBuf.length]),
          hostBuf,
          Buffer.from([(targetPort >> 8) & 0xff, targetPort & 0xff]),
        ]));
      }
    });
  });
}

function httpUpstreamTunnel(proxy, targetHost, targetPort, timeout = CONNECT_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const sock = net.connect(proxy.port, proxy.host);
    const timer = setTimeout(() => { sock.destroy(); reject(new Error("upstream connect timeout")); }, timeout);
    sock.on("error", (e) => { clearTimeout(timer); reject(e); });
    sock.once("connect", () => {
      let head = `CONNECT ${targetHost}:${targetPort} HTTP/1.1\r\nHost: ${targetHost}:${targetPort}\r\n`;
      if (proxy.user) {
        head += `Proxy-Authorization: Basic ${Buffer.from(`${proxy.user}:${proxy.pass}`).toString("base64")}\r\n`;
      }
      head += `Connection: close\r\n\r\n`;
      sock.write(head);
      let buf = "";
      const onData = (chunk) => {
        buf += chunk.toString("utf8");
        if (buf.includes("\r\n\r\n")) {
          sock.removeListener("data", onData);
          clearTimeout(timer);
          const statusLine = buf.split("\r\n")[0] || "";
          const code = parseInt((statusLine.split(" ")[1] || "0"), 10);
          if (code >= 200 && code < 300) resolve(sock);
          else { sock.destroy(); reject(new Error(`upstream rejected CONNECT: ${statusLine}`)); }
        }
      };
      sock.on("data", onData);
    });
  });
}

function directConnect(host, port, timeout = CONNECT_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const sock = net.connect(port, host);
    const timer = setTimeout(() => { sock.destroy(); reject(new Error("direct connect timeout")); }, timeout);
    sock.once("connect", () => { clearTimeout(timer); resolve(sock); });
    sock.once("error", (e) => { clearTimeout(timer); reject(e); });
  });
}

// ---------------------------------------------------------------------------
// Helpers HTTP
// ---------------------------------------------------------------------------
function sendJson(res, code, obj) {
  const body = Buffer.from(JSON.stringify(obj));
  res.writeHead(code, {
    "content-type": "application/json; charset=utf-8",
    "content-length": body.length,
    ...corsHeaders(),
  });
  res.end(body);
}

function readJsonBody(req, limit = JSON_SPEC_LIMIT) {
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
      } catch (e) {
        reject(new Error("invalid json body"));
      }
    });
    req.on("error", reject);
  });
}

function preferredFrom(req, urlObj) {
  return (
    (urlObj && urlObj.searchParams.get("proxy")) ||
    req.headers["x-proxy"] ||
    req.headers["x-upstream-proxy"] ||
    null
  );
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
function dashboardHtml(host) {
  const alive = poolAlive().length;
  const up = Math.floor((Date.now() - stats.startedAt) / 1000);
  return `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Universal Relay Proxy — blitz.cloud</title>
<style>body{font-family:system-ui,sans-serif;max-width:820px;margin:32px auto;padding:0 16px;line-height:1.65}
code,pre{background:#f4f4f5;padding:2px 6px;border-radius:6px}pre{padding:12px;overflow:auto}
.card{border:1px solid #e4e4e7;border-radius:12px;padding:16px;margin:12px 0}
h1{margin-bottom:4px}.muted{color:#71717a}</style></head><body>
<h1>🔀 Universal Relay Proxy</h1>
<p>Status: <b>ONLINE</b> · Pool: <b>${POOL.length}</b> (${alive} alive) · Strategi: <code>${STRATEGY}</code> · Upstream timeout: <code>${UPSTREAM_TIMEOUT_MS === 0 ? "tanpa batas (long request OK)" : UPSTREAM_TIMEOUT_MS + "ms"}</code> · Uptime: ${up}s</p>

<div class="card"><h3>A. Forward proxy (semua app / browser / Puppeteer)</h3>
<pre>http://${host}</pre>
<p>Puppeteer: <code>--proxy-server=http://${host}</code><br>
Python: <code>proxies={"http": "http://${host}", "https": "http://${host}"}</code><br>
curl: <code>curl -x http://${host} https://api.ipify.org</code></p>
<p>Rotasi otomatis tiap request. Kunci ke 1 proxy: header <code>X-Proxy: host:port:user:pass</code> atau <code>X-Proxy: socks5://u:p@h:p</code>.</p></div>

<div class="card"><h3>B. Universal relay — tanpa setting proxy</h3>
<pre># pola umum: METHOD apa pun, header apa pun diteruskan, body di-STREAM
GET  /api/fetch?url=https://api.ipify.org
GET  /r?url=https://api.ipify.org
GET  /proxy/https://api.ipify.org
POST /api/fetch?url=https://httpbin.org/post   (body diteruskan mentah)
POST /api/fetch  {"url":"https://...","method":"POST","headers":{},"body":{...}}</pre>
<p>Contoh OpenAI-style: <code>POST /api/fetch?url=https://api.openai.com/v1/chat/completions</code> dengan header <code>Authorization</code> asli kamu — diteruskan 1:1, support streaming SSE.</p></div>

<div class="card"><h3>Endpoint manajemen</h3>
<ul>
<li><code>GET /health</code> · <code>GET /api/stats</code> · <code>GET /api/proxies</code></li>
<li><code>POST /api/reload</code> — reload pool tanpa restart</li>
<li><code>POST /api/check</code> <code>{"url":"http://httpbin.org/ip","proxy":"..."}</code> — tes proxy</li>
</ul></div>
<p class="muted">Universal: semua method · semua header · streaming up/down · long request · SSE · tanpa auth · CORS *</p>
</body></html>`;
}

// ---------------------------------------------------------------------------
// Router utama (raw http — tanpa framework agar universal)
// ---------------------------------------------------------------------------
async function handleRequest(req, res) {
  const rawUrl = req.url || "/";

  // 1) Forward-proxy mode: request-line absolut (GET http://... )
  if (/^https?:\/\//i.test(rawUrl)) {
    const proxy = pickProxy(req.headers["x-proxy"] || req.headers["x-upstream-proxy"]);
    relayStream(req, res, rawUrl, { proxy });
    return;
  }

  // pisahkan path & query
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

  // CORS preflight — jawab global agar browser OK untuk semua target
  if (req.method === "OPTIONS" && (pathOnly.startsWith("/api/") || pathOnly.startsWith("/proxy") || pathOnly === "/r")) {
    res.writeHead(204, {
      ...corsHeaders(),
      "access-control-max-age": "86400",
      "content-length": "0",
    });
    res.end();
    return;
  }

  // 2) Manajemen
  if (req.method === "GET" && pathOnly === "/") {
    const body = dashboardHtml(req.headers.host || `localhost:${PORT}`);
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "content-length": Buffer.byteLength(body), ...corsHeaders() });
    res.end(body);
    return;
  }
  if (req.method === "GET" && pathOnly === "/health") {
    sendJson(res, 200, {
      ok: true,
      uptimeSec: Math.floor((Date.now() - stats.startedAt) / 1000),
      poolSize: POOL.length,
      alive: poolAlive().length,
      strategy: STRATEGY,
      upstreamTimeoutMs: UPSTREAM_TIMEOUT_MS,
      totalRelay: stats.totalRelay,
      totalConnect: stats.totalConnect,
    });
    return;
  }
  if (req.method === "GET" && pathOnly === "/api/stats") {
    sendJson(res, 200, {
      ok: true,
      uptimeSec: Math.floor((Date.now() - stats.startedAt) / 1000),
      poolSize: POOL.length,
      alive: poolAlive().length,
      strategy: STRATEGY,
      totals: {
        relay: stats.totalRelay,
        connect: stats.totalConnect,
        bytesUp: stats.totalBytesUp,
        bytesDown: stats.totalBytesDown,
      },
      uses: stats.proxyUses,
      fails: stats.proxyFails,
    });
    return;
  }
  if (req.method === "GET" && pathOnly === "/api/proxies") {
    sendJson(res, 200, {
      ok: true,
      count: POOL.length,
      strategy: STRATEGY,
      proxies: POOL.map((p, i) => ({
        index: i,
        masked: maskProxy(p),
        uses: p.uses,
        inCooldown: p.cooldownUntil > Date.now(),
      })),
      loadErrors: LOAD_ERRORS.slice(0, 20),
    });
    return;
  }
  if (req.method === "POST" && pathOnly === "/api/reload") {
    const r = loadPool();
    POOL = r.pool;
    LOAD_ERRORS = r.errors;
    rrIndex = 0;
    sendJson(res, 200, { ok: true, count: POOL.length, errors: LOAD_ERRORS.slice(0, 20) });
    return;
  }
  if (pathOnly === "/api/check") {
    let url = urlObj.searchParams.get("url") || DEFAULT_CHECK_URL;
    let preferred = preferredFrom(req, urlObj);
    const ct = (req.headers["content-type"] || "").toLowerCase();
    if ((req.method === "POST" || req.method === "PUT") && ct.includes("application/json")) {
      try {
        const b = await readJsonBody(req);
        if (b.url) url = b.url;
        if (b.proxy) preferred = b.proxy;
      } catch (e) {
        sendJson(res, 400, { ok: false, error: String(e.message || e) });
        return;
      }
    } else if (req.method !== "GET") {
      // buang body yang mungkin ada agar socket bersih (jangan hang kalau sudah ended)
      if (!req.readableEnded) {
        req.resume();
        await new Promise((r) => {
          req.on("end", r);
          req.on("error", r);
          setTimeout(r, 2000);
        });
      }
    }
    const proxy = pickProxy(preferred);
    if (!proxy) {
      sendJson(res, 400, { ok: false, error: "proxy pool empty" });
      return;
    }
    const t0 = Date.now();
    const isHttps = String(url).startsWith("https");
    const lib = isHttps ? https : http;
    const agent = agentFor(proxy, isHttps);
    try {
      const r = await new Promise((resolve, reject) => {
        const q = lib.request(String(url), { method: "GET", agent }, (rp) => {
          const ch = [];
          rp.on("data", (c) => ch.push(c));
          rp.on("end", () => resolve({ status: rp.statusCode, body: Buffer.concat(ch) }));
        });
        q.on("error", reject);
        q.setTimeout(20000, () => q.destroy(new Error("check timeout")));
        q.end();
      });
      markOk(proxy);
      sendJson(res, 200, {
        ok: r.status >= 200 && r.status < 400,
        via: maskProxy(proxy),
        status: r.status,
        latencyMs: Date.now() - t0,
        sample: r.body.slice(0, 300).toString("utf8"),
      });
    } catch (e) {
      markFail(proxy);
      sendJson(res, 200, { ok: false, via: maskProxy(proxy), latencyMs: Date.now() - t0, error: String((e && e.message) || e) });
    }
    return;
  }

  // 3) Universal relay
  //   /api/fetch?url=TARGET  |  /r?url=TARGET  |  /proxy/TARGET...
  //   method & header & body: diteruskan 1:1 (streaming)
  if (pathOnly === "/api/fetch" || pathOnly === "/r" || pathOnly === "/api/relay") {
    const ct = (req.headers["content-type"] || "").toLowerCase();
    const qUrl = urlObj.searchParams.get("url") || req.headers["x-target-url"];
    if (qUrl) {
      // raw-stream mode: target dari ?url=, sisa query diteruskan ke target
      let target = String(qUrl);
      const extra = [];
      for (const [k, v] of urlObj.searchParams.entries()) {
        if (k === "url" || k === "proxy") continue;
        extra.push(`${encodeURIComponent(k)}=${encodeURIComponent(v)}`);
      }
      if (extra.length) target += (target.includes("?") ? "&" : "?") + extra.join("&");
      const method = req.headers["x-target-method"] || req.method;
      relayStream(req, res, target, { method, preferredProxy: preferredFrom(req, urlObj) });
      return;
    }
    // JSON-spec mode: {url, method, headers, body, timeout, proxy}
    if (ct.includes("application/json")) {
      let b;
      try {
        b = await readJsonBody(req);
      } catch (e) {
        sendJson(res, 400, { ok: false, error: String(e.message || e) });
        return;
      }
      if (!b.url && !b.target) {
        sendJson(res, 400, { ok: false, error: 'missing "url". Kirim ?url= atau JSON {"url":"https://..."}' });
        return;
      }
      const target = String(b.url || b.target);
      const timeout = b.timeout !== undefined ? b.timeout : UPSTREAM_TIMEOUT_MS;
      const forceHeaders = {};
      if (b.headers && typeof b.headers === "object") Object.assign(forceHeaders, b.headers);
      // pastikan body JSON diteruskan sebagai JSON
      let specBody = b.body !== undefined ? b.body : null;
      if (specBody !== null && specBody !== undefined) {
        const hasCT = Object.keys(forceHeaders).some((k) => k.toLowerCase() === "content-type");
        if (!hasCT) forceHeaders["content-type"] = "application/json";
      }
      relayStream(req, res, target, {
        method: b.method || "GET",
        proxy: b.proxy ? pickProxy(b.proxy) : pickProxy(preferredFrom(req, urlObj)),
        headers: {},
        forceHeaders,
        specBody,
        timeout,
      });
      return;
    }
    sendJson(res, 400, { ok: false, error: "missing ?url=. Contoh: /api/fetch?url=https://api.ipify.org" });
    return;
  }

  if (pathOnly === "/proxy" || pathOnly.startsWith("/proxy/")) {
    let rest = pathOnly.slice("/proxy".length);
    if (rest.startsWith("/")) rest = rest.slice(1);
    // decode %XX; sisakan query apa adanya (semua milik target)
    try {
      rest = decodeURIComponent(rest);
    } catch { /* biarkan mentah */ }
    let target = rest;
    if (queryStr) target += "?" + queryStr;
    if (!target || !/^https?:\/\//i.test(target)) {
      sendJson(res, 400, { ok: false, error: "usage: /proxy/https://target-host/path?q=1 (atau /proxy/<url-encoded>)" });
      return;
    }
    relayStream(req, res, target, {
      method: req.headers["x-target-method"] || req.method,
      preferredProxy: req.headers["x-proxy"] || req.headers["x-upstream-proxy"] || null,
    });
    return;
  }

  // 4) 404
  sendJson(res, 404, {
    ok: false,
    error: "not found",
    hint: "GET /api/fetch?url=https://... | /proxy/https://... | set proxy http://host:port di client",
  });
}

// ---------------------------------------------------------------------------
// Server — timeout dimatikan untuk long request / SSE / download besar
// ---------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  handleRequest(req, res).catch((e) => {
    if (!res.headersSent) sendJson(res, 500, { ok: false, error: String((e && e.message) || e) });
    else try { res.destroy(); } catch {}
  });
});

// Long request: jangan putus koneksi idle (SSE, LLM stream, polling lama)
server.requestTimeout = 0;
server.headersTimeout = 120000;
server.keepAliveTimeout = 65000;
server.maxHeadersCount = 100;

// CONNECT — HTTPS tunneling (dengan/tanpa upstream), tunnel mentah long-lived
server.on("connect", async (req, clientSocket, head) => {
  stats.totalConnect++;
  const authority = req.url || "";
  const sep = authority.lastIndexOf(":");
  const targetHost = sep > 0 ? authority.slice(0, sep).replace(/^\[|\]$/g, "") : authority;
  const targetPort = sep > 0 ? parseInt(authority.slice(sep + 1), 10) : 443;
  if (!targetHost || !targetPort) {
    clientSocket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
    return clientSocket.destroy();
  }
  const proxy = pickProxy(req.headers["x-proxy"] || req.headers["x-upstream-proxy"]);
  const onErr = () => {
    try { clientSocket.write("HTTP/1.1 502 Bad Gateway\r\n\r\n"); } catch {}
    try { clientSocket.destroy(); } catch {}
  };
  try {
    let upstream;
    if (!proxy) upstream = await directConnect(targetHost, targetPort);
    else if (proxy.protocol.startsWith("socks")) upstream = await socks5Connect(proxy, targetHost, targetPort);
    else upstream = await httpUpstreamTunnel(proxy, targetHost, targetPort);
    if (proxy) markOk(proxy);
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
    upstream.on("error", () => { if (proxy) markFail(proxy); bye(); });
  } catch {
    if (proxy) markFail(proxy);
    onErr();
  }
});

// WebSocket / Upgrade — teruskan handshake mentah ke target (direct atau via upstream CONNECT)
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
      clientSocket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
      return clientSocket.destroy();
    }
    const t = new URL(targetStr);
    const targetPort = t.port ? parseInt(t.port, 10) : t.protocol.startsWith("https") || t.protocol.startsWith("wss") ? 443 : 80;
    const proxy = pickProxy(req.headers["x-proxy"] || req.headers["x-upstream-proxy"]);
    let upstream;
    if (!proxy) upstream = await directConnect(t.hostname, targetPort);
    else if (proxy.protocol.startsWith("socks")) upstream = await socks5Connect(proxy, t.hostname, targetPort);
    else upstream = await httpUpstreamTunnel(proxy, t.hostname, targetPort);

    // replay handshake: path target + semua header (kecuali kontrol)
    const fwdH = {};
    for (const [k, v] of Object.entries(req.headers || {})) {
      const lk = k.toLowerCase();
      if (["x-proxy", "x-upstream-proxy", "x-target-url"].includes(lk)) continue;
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
  console.log(`[relay] universal proxy listening on ${HOST}:${PORT}`);
  console.log(`[relay] pool=${POOL.length} alive=${poolAlive().length} strategy=${STRATEGY} upstreamTimeout=${UPSTREAM_TIMEOUT_MS === 0 ? "none" : UPSTREAM_TIMEOUT_MS + "ms"}`);
  if (LOAD_ERRORS.length) console.log(`[relay] ${LOAD_ERRORS.length} bad line(s) skipped (GET /api/proxies)`);
  if (POOL.length === 0) console.log("[relay] pool kosong — jalan mode direct. Isi env PROXIES / proxies.txt lalu POST /api/reload.");
});

module.exports = { server, parseProxyLine, pickProxy, relayStream };
