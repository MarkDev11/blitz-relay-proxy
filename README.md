# 🔀 Proxy Server (blitz.cloud)

**Server ini ADALAH proxy-nya.** Bukan tempat nyimpen proxy, bukan rotasi proxy orang lain.
Deploy → dapat URL → pakai URL itu sebagai proxy di app/browser/curl/Python.
IP yang dilihat target = IP egress blitz.cloud.

## Deploy (Dockerfile)

Step 2 blitz: biarkan **Dockerfile**, **Start command kosong** (= pakai `CMD` → `node server.js`) atau isi `node server.js`. Jangan isi `npm start` untuk mode Docker.

Env yang dibaca (Step 3) — cuma 2, opsional:

| Var | Default | Fungsi |
|---|---|---|
| `PROXY_USER` | _(kosong)_ | Kalau diisi + `PROXY_PASS`, proxy terkunci |
| `PROXY_PASS` | _(kosong)_ | Pasangan `PROXY_USER` |

Env lama (`PROXIES`, `PROXY_FILE`, `ROTATE`, `UPSTREAM_TIMEOUT_MS`, dll) **sudah tidak dipakai** — hapus saja kalau masih muncul di dashboard blitz.

## Cara pakai (server ini proxy-nya)

```
http://blitz-relay-proxy.dannd.blitz.cloud
```

```bash
# HTTP
curl -x http://blitz-relay-proxy.dannd.blitz.cloud http://httpbin.org/ip

# HTTPS (CONNECT tunnel)
curl -x http://blitz-relay-proxy.dannd.blitz.cloud https://api.ipify.org
```

- Puppeteer: `args: ["--proxy-server=http://blitz-relay-proxy.dannd.blitz.cloud"]`
- Python: `proxies = {"http": APP, "https": APP}`
- Browser / OS: isi HTTP proxy = host ini.

Dengan auth (`PROXY_USER` + `PROXY_PASS` diisi):

```bash
curl -x http://USER:PASS@blitz-relay-proxy.dannd.blitz.cloud https://api.ipify.org
```

## Tanpa setting proxy (relay universal)

Method + header + body diteruskan mentah, body **streaming** (tanpa buffering), long request / SSE / file besar OK.

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
# tes: curl -x http://localhost:3000 https://api.ipify.org
```
