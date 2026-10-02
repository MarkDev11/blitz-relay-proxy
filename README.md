# 🔀 Proxy Server (blitz.cloud) — 9router-compatible relay

**Server ini ADALAH proxy-nya.** Deploy → dapat URL → daftarkan sebagai **proxy pool di 9router**
(type `vercel`), atau pakai langsung sebagai relay universal.
IP yang dilihat target = IP egress blitz.cloud.

## Cara pakai utama: 9router proxy pool ✅

Di dashboard 9router → **Proxy Pools → Add**:

| Field | Isi |
|---|---|
| `name` | `blitz-relay` (bebas) |
| `proxyUrl` | `https://proxy.dannd.blitz.cloud` |
| `type` | `vercel` |
| `noProxy` | _(kosongkan)_ |

Lalu pakai pool itu di connection (per-connection `proxyPoolId`).
Cara kerja: 9router `fetch(poolUrl, { headers: { "x-relay-target": "https://target-host",
"x-relay-path": "/path?q=1", ...header asli } })` — server teruskan method + header
(minus `x-relay-*`) + body 1:1 secara streaming, response target dibalikkan mentah
(status + headers + body). Protokolnya identik dengan worker relay resmi 9router
(cloudflare/vercel/deno deploy), jadi `Test` di dashboard 9router juga jalan
(`x-relay-target:https://httpbin.org` + `x-relay-path:/get`).

Tes manual (hasilnya harus JSON httpbin, bukan dashboard):

```bash
curl -s https://APP/ -H "x-relay-target: http://httpbin.org" -H "x-relay-path: /ip"
curl -s -X POST https://APP/ -H "x-relay-target: https://httpbin.org" \
  -H "x-relay-path: /post" -H "Content-Type: application/json" -d '{"a":1}'
```

> Catatan: mode forward-proxy klasik (`curl -x`, set proxy di browser/OS/Puppeteer,
> `CONNECT`) tetap ada di kode tapi **tidak lolos Cloudflare di depan blitz.cloud**
> (request absolut/`CONNECT` dibuang edge, tidak sampai ke container). Di VPS tanpa
> CDN di depan, mode itu langsung jalan tanpa ubah kode.

## Relay universal (tanpa 9router)

Method + header + body diteruskan mentah, body **streaming** (tanpa buffering),
long request / SSE / file besar OK.

```bash
curl "https://APP/api/fetch?url=https://api.ipify.org"
curl "https://APP/r?url=https://api.ipify.org"
curl "https://APP/proxy/https://api.ipify.org"

curl -X POST "https://APP/api/fetch?url=https://httpbin.org/post" \
  -H "Content-Type: application/json" -d '{"hello":"world"}'

# OpenAI-style + SSE stream
curl -N -X POST "https://APP/api/fetch?url=https://api.openai.com/v1/chat/completions" \
  -H "Authorization: Bearer sk-..." -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true}'

# JSON-spec
curl -X POST https://APP/api/fetch -H "Content-Type: application/json" \
  -d '{"url":"https://httpbin.org/anything","method":"PUT","headers":{"x-custom":"1"},"body":{"a":1}}'
```

## Deploy (Dockerfile)

Step 2 blitz: biarkan **Dockerfile**, **Start command kosong** (= pakai `CMD` → `node server.js`)
atau isi `node server.js`. Jangan isi `npm start` untuk mode Docker.

Env yang dibaca (Step 3) — cuma 2, opsional:

| Var | Default | Fungsi |
|---|---|---|
| `PROXY_USER` | _(kosong)_ | Kalau diisi + `PROXY_PASS`, relay terkunci (401/407) |
| `PROXY_PASS` | _(kosong)_ | Pasangan `PROXY_USER` |

## Manajemen

| Endpoint | Fungsi |
|---|---|
| `GET /` | Dashboard |
| `GET /health` | `{"ok":true,"proxy":true,...}` |
| `GET /api/stats` | Statistik forward/CONNECT/relay |

## Lokal

```bash
npm install
npm start
# http://localhost:3000
# tes protokol 9router:
# curl -s http://localhost:3000/ -H "x-relay-target: http://httpbin.org" -H "x-relay-path: /ip"
```
