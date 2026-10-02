# 🔀 Universal Relay Proxy

Forward proxy + universal HTTP relay siap deploy di **blitz.cloud** (plain Node.js, tanpa framework). Tanpa auth, CORS terbuka.

Prinsip: **universal** — method apa pun, header apa pun, body apa pun, durasi apa pun.

- ✅ Semua HTTP method (`GET POST PUT PATCH DELETE HEAD OPTIONS` + custom)
- ✅ Semua header diteruskan 1:1 (kecuali hop-by-hop), termasuk `Authorization`, custom `X-*`
- ✅ Body **streaming** — tanpa buffering, tanpa limit MB → file besar, upload, SSE aman
- ✅ Long request — upstream timeout default **0 (tanpa batas)**; `server.requestTimeout = 0`
- ✅ SSE / LLM stream / download besar — `proxyRes.pipe(clientRes)` + `x-accel-buffering: no`
- ✅ WebSocket upgrade (`ws://`/`wss://` via `/proxy/` & `CONNECT`)
- ✅ Forward proxy: `CONNECT` (HTTPS) + absolute-URI (HTTP) di port yang sama
- ✅ Pool upstream rotasi `roundrobin`/`random` + cooldown + sticky `X-Proxy`
- ✅ CORS preflight `OPTIONS` dijawab global

## Format proxy (pool upstream)

| Format | Contoh |
|---|---|
| `protocol://user:pass@host:port` | `http://u:p@1.2.3.4:8080`, `socks5://u:p@1.2.3.4:1080` |
| `protocol://host:port` | `http://1.2.3.4:8080` |
| `user:pass@host:port` | `u:p@1.2.3.4:8080` → http |
| `host:port:user:pass` | `1.2.3.4:8080:u:p` |
| `host:port` | `1.2.3.4:8080` |

Protocol: `http`, `https`, `socks4`, `socks5`, `socks5h`.

## Deploy blitz.cloud

1. Push folder `relay/` sebagai repo / subfolder.
2. New App → dari GitHub → start command `npm start` (jangan set `PORT` manual).
3. Env di dashboard:
   - `PROXIES=http://u:p@1.2.3.4:8080,1.2.3.4:8080:u:p`
   - `ROTATE=roundrobin`
   - `UPSTREAM_TIMEOUT_MS=0`
4. Deploy. Buka URL app → dashboard.

Alternatif: isi `proxies.txt` lalu push, atau build `Dockerfile` sebagai container.

## Cara pakai

### A. Forward proxy (semua app)

```
http://<app>.blitz.cloud
```

```bash
curl -x http://<app>.blitz.cloud https://api.ipify.org
curl -x http://<app>.blitz.cloud http://httpbin.org/ip
```

- Puppeteer: `args: ["--proxy-server=http://<app>.blitz.cloud"]`
- Python: `proxies={"http": APP, "https": APP}`
- Kunci ke 1 upstream: header `X-Proxy: host:port:user:pass` atau `X-Proxy: socks5://u:p@h:p`.

### B. Universal relay (tanpa setting proxy di client)

```bash
# GET — query SELAIN url/proxy diteruskan ke target
curl "https://<app>.blitz.cloud/api/fetch?url=https://api.ipify.org"
curl "https://<app>.blitz.cloud/r?url=https://api.ipify.org"
curl "https://<app>.blitz.cloud/proxy/https://api.ipify.org"

# POST mentah — method/header/body diteruskan 1:1 (streaming)
curl -X POST "https://<app>.blitz.cloud/api/fetch?url=https://httpbin.org/post" \
  -H "Content-Type: application/json" -d '{"hello":"world"}'

# OpenAI-style + streaming SSE (header Authorization diteruskan apa adanya)
curl -N -X POST "https://<app>.blitz.cloud/api/fetch?url=https://api.openai.com/v1/chat/completions" \
  -H "Authorization: Bearer sk-..." -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true}'

# JSON-spec (tanpa query): url+method+headers+body
curl -X POST https://<app>.blitz.cloud/api/fetch \
  -H "Content-Type: application/json" \
  -d '{"url":"https://httpbin.org/anything","method":"PUT","headers":{"x-custom":"1"},"body":{"a":1}}'

# Kunci upstream tertentu
curl "https://<app>.blitz.cloud/api/fetch?url=https://api.ipify.org&proxy=1.2.3.4:8080:u:p"
curl -H "X-Proxy: socks5://u:p@h:p" "https://<app>.blitz.cloud/api/fetch?url=https://api.ipify.org"
```

### Manajemen

| Endpoint | Fungsi |
|---|---|
| `GET /` | Dashboard |
| `GET /health` | Hidup + pool |
| `GET /api/stats` | Statistik relay per upstream |
| `GET /api/proxies` | Daftar upstream (password di-mask) + baris format salah |
| `POST /api/reload` | Reload pool tanpa restart |
| `POST /api/check` | Tes upstream: `{"url":"http://httpbin.org/ip","proxy":"..."}` |

## Lokal

```bash
cd relay
npm install
npm start
# http://localhost:3000
```

## Env

| Var | Default | Fungsi |
|---|---|---|
| `PORT` | `3000` | Di-inject blitz otomatis |
| `HOST` | `0.0.0.0` | Bind |
| `PROXIES` | — | Pool inline (koma/newline/semicolon) |
| `PROXY_FILE` | `proxies.txt` | File pool |
| `ROTATE` | `roundrobin` | `roundrobin` \| `random` |
| `UPSTREAM_TIMEOUT_MS` | `0` | 0 = tanpa batas (long request OK) |
| `CONNECT_TIMEOUT_MS` | `20000` | Timeout handshake CONNECT upstream |
| `CHECK_URL` | `http://httpbin.org/ip` | Default `/api/check` |
| `PROXY_COOLDOWN_MS` | `60000` | Cooldown upstream gagal |
| `PROXY_MAX_FAILS` | `3` | Batas gagal sebelum cooldown |
