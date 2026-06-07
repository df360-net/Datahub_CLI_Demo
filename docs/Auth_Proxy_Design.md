# DataHub Auth Proxy — Design

This document specifies the local Auth Proxy sidecar for `datahub_proxy_app1`. It is the practical companion to [`DataHub_Auth_Proxy_Pattern.md`](DataHub_Auth_Proxy_Pattern.md), which explains *why* the pattern exists. This doc explains *what to build*.

The proxy is the **single piece of infrastructure** that makes `datahub_proxy_app1` a faithful rehearsal of the workplace pattern. Get this right; the rest of the application can stay auth-naive.

**Decisions (settled 2026-06-07):** Python implementation, separate git repo from DF360, **session auth only** (no PAT mode), plain OS process (not Docker for v1), **OpenAPI endpoints only** (no GraphQL forwarding in v1), and **every API call checks status — never dump-and-forget.**

**Stack alignment (2026-06-07):** The proxy and the sample ETL share Jianmin's workplace Python stack: `requests`, `urllib3`, `pydantic`, `python-dotenv`, plus `sqlalchemy` + `pyodbc` for the ETL's MSSQL access and `acryl-datahub` for URN/aspect helpers. **The proxy adds zero extra dependencies — it uses Python's stdlib `http.server.ThreadingHTTPServer`**, mirroring the workplace's "plain Python web server" choice. **No `httpx`, no `fastapi`, no `flask`, no `waitress`, no `pydantic-settings`.** This ensures lab code transfers verbatim to work.

---

## 1. Goals and Non-Goals

### Goals

| | Why |
|---|---|
| **Single localhost endpoint** for the application to call | App code never sees the real DataHub URL or any credential |
| **Session auth handled internally** — login, cookie attach, refresh, 401-recovery | The whole point of the rehearsal — application stays auth-naive |
| **Pass-through routing** — any DataHub path the app calls is forwarded verbatim to upstream | The proxy is not a translation layer; it is a credential layer |
| **Transparent body handling** (including streaming, gzip, multi-MB payloads) | Catalog batches and assertion payloads can be large |
| **Loopback-only binding** | Credentials never leave the host except via TLS to DataHub |
| **One proxy per application** | Each application team's workload owns its own proxy instance, its own credentials, its own host. No shared / multi-tenant proxy. |
| **Verifiable status on every upstream call** | The proxy itself checks each upstream HTTP status; never silently swallows failures; surfaces them to the caller and to logs |
| **Minimal operational footprint** — one process, one config file, one log stream | The whole point is that it's invisible to teams using it |

### Non-Goals

| | Why not |
|---|---|
| PAT (Bearer token) auth | Out of scope. The lab rehearsal mirrors the workplace, which uses SSO/session only. |
| GraphQL forwarding (in v1) | OpenAPI v3 covers everything a typical ETL needs: catalog push, lineage emission, assertion publishing, run events. Keep it simple. Reconsider only if a real need arises. |
| Request body transformation | The decoupled producer/consumer model says the *payload* is the contract; the proxy must not alter it |
| Response caching | DataHub is the authoritative metadata store; cached responses can lie |
| Business logic (e.g., assertion shape validation) | The proxy is auth, not policy |
| Multi-tenant (multiple users on one proxy) | One proxy per application. Multi-tenancy belongs at DataHub's IdP, not here |
| Auto-retry beyond the single auth-refresh case | Retries are an application concern; the proxy fails fast and surfaces the status |
| HTTPS termination on the proxy's listener | The application talks to localhost (no need); upstream TLS is handled by the proxy's HTTP client |

---

## 2. The Interface (What the Application Sees)

```
┌────────────────────────────────────────────────────────────────┐
│  Application (one ETL workload)                                 │
│    POST http://127.0.0.1:8080/openapi/v3/entity/assertion      │
│      Content-Type: application/json                            │
│      <body>                                                    │
│    NO Authorization header                                     │
│    NO Cookie header                                            │
└────────────────────────────────────────────────────────────────┘
                                │
                                ▼ TCP loopback only
┌────────────────────────────────────────────────────────────────┐
│  Auth Proxy (per-application, listens on 127.0.0.1:8080)        │
│                                                                │
│  1. Receive request                                            │
│  2. Strip any incoming Authorization / Cookie headers          │
│  3. Inject the managed cookie jar (PLAY_SESSION + actor + …)    │
│  4. Forward to DataHub frontend over TLS                       │
│  5. Check upstream status; on 401, re-login once and replay    │
│  6. Stream response back; status preserved verbatim            │
│  7. Log the exchange (no body, no credentials)                 │
└────────────────────────────────────────────────────────────────┘
                                │
                                ▼ TLS (or http in lab) to upstream
┌────────────────────────────────────────────────────────────────┐
│  DataHub Frontend                                               │
│    http://192.168.0.16:9002    (Murphy lab)                    │
│    https://datahub.example.com (work — over corporate TLS)     │
│                                                                │
│  Frontend authenticates the session, forwards to GMS internally│
└────────────────────────────────────────────────────────────────┘
```

**The application's view is dead simple**: `http://127.0.0.1:<PROXY_PORT>` is "the DataHub I talk to." No auth headers, no cookie management, no awareness of the real DataHub URL.

### Why upstream is the FRONTEND, not GMS directly

In DataHub's architecture, the **frontend (port 9002 on Murphy)** is the session-auth gateway:

- `POST /logIn` accepts username + password, returns `Set-Cookie: PLAY_SESSION=...` **and** `Set-Cookie: actor=urn:li:corpuser:<user>` — the frontend's auth filter requires both on subsequent requests
- Subsequent requests to `/openapi/v3/...`, `/api/v2/...`, etc. are accepted with that cookie and proxied internally to GMS (port 8080)

GMS (port 8080) directly accepts Bearer tokens (PAT) but **not** the PLAY_SESSION cookie — sessions live at the frontend. Since we're session-only, our upstream must be the frontend.

### Endpoints the proxy supports (v1)

| Path pattern | Behavior |
|---|---|
| `/proxy/**` | **Local** — proxy's own endpoints (`/proxy/healthz`). NOT forwarded. |
| Everything else | Forwarded to `<DATAHUB_URL>/<path>`, session cookie injected |

The proxy is **transparent pass-through** except for the reserved `/proxy/**` prefix. This is broader than a strict allow-list, but necessary because the `acryl-datahub` SDK emitter targets several endpoint families (`/openapi/v3/**`, `/openapi/v2/**`, `/api/v2/**`, `/aspects/**`, `/entities/**`, `/config`, etc.). Enumerating them is brittle. Auth is the proxy's concern; URL policing belongs at DataHub itself.

The `/proxy/*` prefix is reserved for proxy-local operations.

> **Frontend routing reality (verified Murphy 2026-06-07).** The DataHub frontend exposes the **OpenAPI** API at the root (`/openapi/v3/...` works directly) but mounts the **Restli/GMS** API under a `/api/gms/` prefix (`/aspects?action=ingestProposal` → **404** at root, but **200** at `/api/gms/aspects?...`). The proxy stays a dumb pass-through — it forwards whatever path it's given — so the *caller* picks the right path:
> - **Option A (`requests` + OpenAPI v3):** call `PROXY_URL/openapi/v3/...` directly.
> - **Option B (`acryl-datahub` `DatahubRestEmitter`):** point it at **`PROXY_URL/api/gms`** (the SDK defaults to the Restli `/aspects` endpoint, which only exists under `/api/gms`). `gms_server="http://127.0.0.1:8080/api/gms"`.

---

## 3. The Session Handler (Sole Auth Mode in v1)

### Config

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATAHUB_URL` | yes | — | Upstream DataHub frontend URL (e.g., `http://192.168.0.16:9002` for Murphy, `https://datahub.example.com` for work) |
| `DATAHUB_USER` | yes | — | Username for `POST /logIn` |
| `DATAHUB_PW` | yes | — | Password for `POST /logIn` |
| `SESSION_REFRESH_MINUTES` | no | `20` | Proactive re-login cadence; default chosen well below DataHub's typical session TTL (~24h) for resilience |

### Lifecycle

1. **At proxy start**: perform login eagerly
   - `POST <DATAHUB_URL>/logIn` with form-encoded `username` + `password`, **using a `requests.Session`** so all `Set-Cookie` values are captured into the session's cookie jar automatically
   - **Check status**: must be 200; if not, log the upstream status + redacted body preview and refuse to start
   - **Check the cookie jar**: must contain `PLAY_SESSION`; if not, refuse to start
   - Store the **whole cookie jar** + a timestamp in memory under a `threading.Lock`. Do NOT single out `PLAY_SESSION` — DataHub's frontend requires the `actor` cookie alongside it (verified against Murphy 2026-06-07: `PLAY_SESSION` alone → 401; `PLAY_SESSION` + `actor` → 200). Storing the full jar is both correct here and robust to DataHub version differences.
2. **Per request**: replay **all** stored cookies on the outgoing request (i.e. attach the managed `requests.Session`'s cookie jar — `PLAY_SESSION` *and* `actor`, plus any others the frontend set)
3. **On upstream `401`**: re-login once
   - Acquire the lock (other concurrent requests wait briefly)
   - POST `/logIn` again; check status; replace the stored cookie jar
   - Replay the original request with the new jar
   - If the retry also returns 401, give up and return 401 to the caller (don't loop)
4. **Background refresh**: a timer re-logs in every `SESSION_REFRESH_MINUTES` minutes to keep the cookie warm even when traffic is sparse
5. **At shutdown**: optional `POST /logOut`; not strictly necessary but tidies the audit trail

### Concurrency

All reads/writes of the stored cookie jar happen under a `threading.Lock` (`ThreadingHTTPServer` spawns one thread per request). This prevents a thundering herd of concurrent re-login attempts when many in-flight requests all see a 401 at the same time. The background refresh timer runs in a daemon `threading.Thread`.

### Auth handler abstraction (still worth keeping)

Even though v1 only has Session, structure the code around an `AuthHandler` ABC so adding a different mode later (OIDC client credentials, mTLS, etc.) is mechanical, not invasive:

```python
class AuthHandler(ABC):
    @abstractmethod
    def inject(self, req_headers: dict) -> None: ...

    @abstractmethod
    def on_upstream_unauthorized(self) -> bool:
        """Called on upstream 401. Return True if re-auth happened
        and the caller should retry; False to surface the 401 as-is."""

    def start(self) -> None: ...
    def stop(self) -> None: ...
```

v1 ships exactly one implementation: `SessionHandler`. The ABC is documentation as much as it is code structure.

---

## 4. Request Lifecycle (Per Forwarded Request)

```
1. Receive request from app on 127.0.0.1:PROXY_PORT
2. Validate path against allow-list (Section 2). If not allowed → 404.
3. Build outgoing request:
     URL    = DATAHUB_URL + original_path + original_querystring
     Method = same
     Headers = copied from incoming, MINUS:
       - Host           (rewritten to upstream host)
       - Authorization  (stripped — proxy controls auth)
       - Cookie         (stripped — proxy controls auth)
     Body   = streamed verbatim
4. AuthHandler.inject(outgoing_request) — attaches the managed cookie jar (PLAY_SESSION + actor + …)
5. Send outgoing request to upstream (streaming if body > 1 MB)
6. ALWAYS check the upstream status:
     If 200..299  → proceed (response is the upstream's)
     If 401       → AuthHandler.on_upstream_unauthorized() may re-auth + retry once
     If 502/503/504 / network error → log + return same status to caller
     If 400/404/etc → return status verbatim; do NOT mask or transform
7. Stream response (status, headers MINUS Set-Cookie, body) back to app
8. Log: timestamp, method, path, upstream status, latency, in-flight request count
   (NEVER log credentials, cookies, request body, or response body at INFO)
```

### Status-check discipline (explicit)

> **Every upstream call has its HTTP status checked. The proxy never assumes success.**

Concretely, this means:

1. **Login POST**: status must be 200 AND the resulting cookie jar must contain `PLAY_SESSION` (the frontend also sets `actor`; both are stored and replayed). Any other shape = login failed; refuse to use the resulting state. Log the upstream status code (and a short body preview, with any credentials redacted).
2. **Forwarded request**: the proxy inspects the upstream status before returning. It does not branch on status (it surfaces verbatim), but it **logs** the status and counts it (success/error counters for observability).
3. **Health endpoint**: `GET /proxy/healthz` actively pings upstream (`GET /config` is a good ping target — light, doesn't require auth at the frontend layer) and reports the result. Stale "I successfully started 12 hours ago" is not health — current reachability is.

This discipline mirrors what we expect of the application code that uses the proxy: **the application must check the proxy's response status on every call. The proxy preserves the status faithfully so this check is meaningful.** "Just POST and assume it worked" is forbidden at every layer.

---

## 5. Header Handling Detail

| Header (incoming from app) | Action |
|---|---|
| `Host` | Replaced with upstream host |
| `Authorization` | **Stripped** (defense against credential confusion / leak) |
| `Cookie` | **Stripped, then replaced** with the proxy's managed jar (`PLAY_SESSION` + `actor` + any other `/logIn` cookies) |
| `Content-Type`, `Content-Length`, `Content-Encoding` | Forwarded verbatim |
| Anything else | Forwarded verbatim |

| Header (incoming from upstream) | Action |
|---|---|
| `Set-Cookie` | **Stripped** (cookies are proxy-internal state, never propagated to app) |
| `Status code` | Preserved verbatim |
| Body | Streamed back verbatim |
| Anything else | Forwarded verbatim |

---

## 6. Operational Shape

### One proxy per application

The expected deployment shape is **one proxy process per application workload**, on the application's own host. Each application team:

- Runs its own proxy
- Owns its own DataHub credentials (the proxy's config)
- Talks to it on its own loopback (different apps can use different ports if they share a host)

There is no shared proxy across applications. This isolates blast radius — a credential leak or proxy compromise affects one team's workload, not the firm.

### Plain Python process (v1)

```
cd C:\Users\jianm\DEV\datahub_proxy_app1
python -m proxy
```

For long-running setups:
- **Windows**: a Scheduled Task with "At startup" trigger + restart on failure
- **Linux**: a systemd user unit

A dev workflow is just "run it in a separate terminal."

### No Docker (in v1)

Plain process keeps iteration fast. Once the proxy is stable, dockerizing is a small follow-on increment (and aligns better with the per-application-host pattern in containerized deployments).

### Health check

```
curl http://127.0.0.1:8080/proxy/healthz

200 OK
{
  "status": "ok",
  "upstream": "reachable",
  "upstream_url": "http://192.168.0.16:9002",
  "session_age_seconds": 412,
  "session_refresh_minutes": 20,
  "last_login_status": 200
}

503 Service Unavailable
{
  "status": "degraded",
  "upstream": "unreachable",
  "upstream_url": "http://192.168.0.16:9002",
  "last_error": "POST /logIn returned 401 (Unauthorized)"
}
```

A supervisor (Scheduled Task, systemd, or a simple shell loop) polls this; CI smoke-tests use it.

---

## 7. End-to-End Sequence (Session Mode)

```
App                Proxy                          DataHub Frontend
                   │ (proxy starts — eager login)  │
                   │ POST /logIn (user, password)  │
                   │ ─────────────────────────────►│
                   │ ◄──── 200 + Set-Cookie ────  │
                   │ status checked, cookie stored,│
                   │ refresh timer started         │

 │  POST /openapi/v3/entity/assertion              │
 │  (no auth)                                      │
 │ ───────────► │                                  │
 │              │ inject Cookie: PLAY_SESSION + actor│
 │              │ ────────────────────────────────►│
 │              │ ◄──────────────────── 200 ─────  │
 │              │ status logged                    │
 │ ◄─── 200 ── │                                  │
 │ APP CHECKS STATUS ← required discipline         │

                   ⏰ refresh timer fires (every 20 min)
                   │ POST /logIn                   │
                   │ ─────────────────────────────►│
                   │ ◄──── 200 + Set-Cookie ────  │
                   │ swap stored cookie under lock │

 │  POST /openapi/v3/entity/assertion              │
 │ ───────────► │                                  │
 │              │ inject (cookie may be stale)     │
 │              │ ────────────────────────────────►│
 │              │ ◄──────────────────── 401 ─────  │ (race: session died early)
 │              │ on_upstream_unauthorized():       │
 │              │   POST /logIn ──────────────────►│
 │              │ ◄──── 200 + new Set-Cookie ───   │
 │              │ retry original ────────────────►│
 │              │ ◄──────────────────── 200 ─────  │
 │ ◄─── 200 ── │                                  │
 │ APP CHECKS STATUS ← still required              │

                   (at shutdown — optional)
                   │ POST /logOut                  │
                   │ ─────────────────────────────►│
                   │ ◄────────────────── 200 ────  │
```

---

## 8. Implementation Choices

### Language: Python — workplace stack, stdlib HTTP server

**Decided** — Python + stdlib `http.server.ThreadingHTTPServer` + `requests` + `pydantic` + `python-dotenv`. The workplace's own Auth Proxy uses a plain Python web server (no framework); we mirror that choice exactly. Sync end-to-end: `requests` (sync HTTP client to upstream) inside a thread-per-request stdlib handler. The whole proxy should fit in ~80–120 lines of Python.

### Dependencies — full repo (`requirements.txt`)

```
# Matches workplace requirements.txt verbatim — no framework deps.
acryl-datahub>=0.13
python-dotenv>=1.0.0
requests>=2.31.0
urllib3>=2.0.0
pyodbc>=5.0.0
pydantic>=2.0.0
sqlalchemy>=2.0.0
```

The proxy needs only `requests`, `urllib3`, `pydantic`, `python-dotenv` (plus stdlib). The ETL needs the full list including `acryl-datahub`, `sqlalchemy`, `pyodbc`. A single `requirements.txt` at the repo root keeps the venv simple.

**Deliberately NOT used:** `httpx`, `fastapi`, `uvicorn`, `flask`, `waitress`, `pydantic-settings`, `aiohttp`. None are in the workplace stack.

### Server skeleton (stdlib)

```python
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class ProxyHandler(BaseHTTPRequestHandler):
    def do_GET(self):    self._forward()
    def do_POST(self):   self._forward()
    def do_PUT(self):    self._forward()
    def do_DELETE(self): self._forward()
    def do_PATCH(self):  self._forward()

    def _forward(self):
        if self.path.startswith("/proxy/"):
            return self._handle_local()
        # build outgoing request, inject auth, call upstream with requests.Session,
        # stream response back via self.wfile.
        ...

if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", PROXY_PORT), ProxyHandler)
    server.serve_forever()
```

`ThreadingHTTPServer` (Python 3.7+, stdlib) spawns one thread per request — fits the proxy's concurrent-but-low-traffic shape perfectly. No `__init__` hooks needed for upstream/auth state — store them as class attributes on the handler.

### Project layout

```
datahub_proxy_app1/                    ← separate git repo
├── proxy/
│   ├── __init__.py
│   ├── __main__.py          # python -m proxy entry point (runs ThreadingHTTPServer)
│   ├── config.py            # env loading via python-dotenv + a pydantic BaseModel
│   ├── server.py            # ProxyHandler(BaseHTTPRequestHandler) + dispatch
│   ├── upstream.py          # requests.Session wrapper, status-checking
│   ├── auth/
│   │   ├── __init__.py
│   │   ├── base.py          # AuthHandler ABC (sync)
│   │   └── session.py       # SessionHandler — POST /logIn, manages the cookie jar (PLAY_SESSION + actor)
│   ├── health.py            # /proxy/healthz
│   └── logging_setup.py
├── tests/
│   ├── test_session.py
│   ├── test_passthrough.py
│   ├── test_status_checks.py
│   └── test_loopback_guard.py
├── docs/                    # design + pattern docs (this folder)
├── .env.example
├── .gitignore
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

## 9. Testing Strategy

| Test | Mechanism |
|---|---|
| **Eager login succeeds** | Mock upstream returns 200 + Set-Cookie; assert proxy starts cleanly and cookie is stored |
| **Eager login fails (bad creds)** | Mock upstream returns 401; assert proxy refuses to start (exit code non-zero) with clear log message |
| **Eager login fails (no Set-Cookie)** | Mock upstream returns 200 but no cookie; assert proxy refuses to start |
| **Pass-through with cookie** | Send `POST /openapi/v3/foo`; assert upstream sees cookie attached and path forwarded verbatim |
| **Status preserved verbatim** | Mock upstream returns 400/404/500; assert proxy returns the same status to the caller |
| **401 → re-login + replay** | Mock upstream returns 401 once then 200; assert proxy re-logs in and replays the original request once |
| **Double 401 doesn't loop** | Mock upstream returns 401 to both original and retry; assert proxy returns 401 to caller without infinite retry |
| **Body integrity** | Send a binary body; assert upstream receives identical bytes (no transformation) |
| **Header strip** | Send incoming request with `Authorization: Bearer leaked` and `Cookie: stale=value`; assert upstream sees NEITHER, only the proxy-injected managed jar (`PLAY_SESSION` + `actor`) |
| **Disallowed path** | Send `POST /api/graphql`; assert proxy returns 404 (not forwarded in v1) |
| **Loopback enforcement** | Start proxy with `PROXY_BIND=0.0.0.0` and no override flag; assert it refuses to start |
| **Concurrent re-login under 401 storm** | Fire 50 concurrent requests, all hit 401; assert exactly one re-login POST (lock works) and all requests succeed on retry |
| **Health endpoint** | `GET /proxy/healthz` returns 200 with upstream reachable, 503 when not |
| **Refresh timer** | Set `SESSION_REFRESH_MINUTES=0.05` (3 sec) in test; assert background re-login fires |

Integration test against Murphy's real DataHub frontend is a separate manual smoke test, not part of the unit suite.

---

## 10. Security Checklist

Before declaring the proxy ready (even for the lab):

- [ ] Bind to `127.0.0.1` by default; refuse non-loopback unless explicit `PROXY_ALLOW_NON_LOOPBACK=1` set
- [ ] Strip incoming `Authorization` and `Cookie` headers before forwarding
- [ ] Strip outgoing `Set-Cookie` before returning to app
- [ ] Never log request bodies or response bodies at INFO level (DEBUG only, behind a flag)
- [ ] Never log credentials, cookie values, or passwords at any level (redact in error paths too)
- [ ] Verify upstream TLS certificate by default; allow opt-out only via explicit `UPSTREAM_VERIFY_TLS=false`
- [ ] Allow `http://` upstream only when the upstream host is a private/loopback address (Murphy is `192.168.0.16` — fine). Refuse `http://` to a public hostname.
- [ ] Provide a `.env.example` with placeholder values; ensure real `.env` is gitignored
- [ ] No secrets in commit history (use `git-secrets` or similar)

---

## 11. Config Reference (Complete)

| Variable | Required | Default | Description |
|---|---|---|---|
| `PROXY_PORT` | no | `8080` | Local port to listen on |
| `PROXY_BIND` | no | `127.0.0.1` | Bind address. Refuses non-loopback unless override set |
| `PROXY_ALLOW_NON_LOOPBACK` | no | `0` | Safety override; set to `1` to permit `0.0.0.0`. Discouraged. |
| `DATAHUB_URL` | yes | — | Upstream DataHub frontend URL (e.g., `http://192.168.0.16:9002`) |
| `DATAHUB_USER` | yes | — | Username for `POST /logIn` |
| `DATAHUB_PW` | yes | — | Password for `POST /logIn` |
| `SESSION_REFRESH_MINUTES` | no | `20` | Proactive re-login cadence |
| `UPSTREAM_TIMEOUT_SECONDS` | no | `60` | Per-request timeout to DataHub |
| `UPSTREAM_VERIFY_TLS` | no | `true` | TLS cert verification on the upstream client |
| `LOG_LEVEL` | no | `INFO` | `DEBUG`/`INFO`/`WARN`/`ERROR` |
| `LOG_FORMAT` | no | `text` | `text` or `json` |
| `LOG_INCLUDE_BODIES` | no | `false` | DEBUG-only flag to include request/response bodies in logs (testing only) |

---

## 12. What "Done" Looks Like for v1

A minimal working proxy where this end-to-end scenario passes against Murphy:

**Option A — direct `requests` (hand-built payload):**

```python
# Application code: no credentials, no real DataHub URL
import requests

resp = requests.post(
    "http://127.0.0.1:8080/openapi/v3/entity/assertion",
    json=[{...assertion entity...}],
    timeout=30.0,
)

# REQUIRED DISCIPLINE — check the status
if resp.status_code != 200:
    raise RuntimeError(f"DataHub publish failed: {resp.status_code} {resp.text[:200]}")
```

**Option B — `acryl-datahub` SDK emitter pointed at the proxy:**

```python
# The SDK uses `requests` internally; pointing it at the proxy works seamlessly.
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass

emitter = DatahubRestEmitter(gms_server="http://127.0.0.1:8080/api/gms", token="")
# /api/gms → the frontend mounts the Restli API there (the SDK's default
#   /aspects endpoint 404s at the root). See "Frontend routing reality" in §2.
# token="" → SDK omits Authorization header; proxy injects the session cookies

mcp = MetadataChangeProposalWrapper(
    entityUrn="urn:li:dataset:(urn:li:dataPlatform:mssql,DCF_DB.CARD_STG.card_auth,PROD)",
    aspect=DatasetPropertiesClass(name="card_auth", description="..."),
)
emitter.emit(mcp)
# SDK raises on non-2xx; the discipline is built in.
```

Both patterns work through the proxy. The ETL uses Option B for catalog/lineage (typed aspect classes are easier than hand-built JSON) and Option A for the OpenAPI v3 assertion endpoints where the SDK's coverage is thinner.

…and:

1. The assertion lands in Murphy's DataHub (visible at http://192.168.0.16:9002)
2. The proxy logs the exchange (without leaking the PLAY_SESSION cookie or the password)
3. `curl http://127.0.0.1:8080/proxy/healthz` returns 200 with the session age
4. Killing the proxy mid-flow and restarting it works — eager login fires, sessions resume
5. All tests in Section 9 pass

Once that's true, the proxy is shippable. v2 adds Docker. v3 adds OIDC mode if/when the workplace needs it.

---

## 13. The Design in One Sentence

> **A single Python process per application, listening on loopback, that performs eager session login to the DataHub frontend, attaches the managed session cookie jar (`PLAY_SESSION` + `actor`) to every forwarded OpenAPI request, re-logs in on 401, and checks the HTTP status of every upstream call — making the producer application's code identical between the Murphy lab and the workplace SSO environment.**

---

*Companion: [`DataHub_Auth_Proxy_Pattern.md`](DataHub_Auth_Proxy_Pattern.md) — the architectural framing. This doc translates that pattern into an implementable spec.*
