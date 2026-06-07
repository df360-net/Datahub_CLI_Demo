# DataHub Auth Proxy Pattern

This document captures the **Auth Proxy** pattern as deployed in enterprise DataHub installations where SSO is the firm-wide authentication standard. It explains why the pattern exists, what architectural consequences it has, and how applications should be built to fit it.

The pattern is observed in real production deployments and is the operational reality for any team integrating with a DataHub instance that lives behind corporate SSO.

---

## 1. The Two Auth Models for DataHub

DataHub supports multiple authentication modes. For programmatic integration there are effectively two camps:

| | Token auth (PAT) | Session auth (SSO-backed) |
|---|---|---|
| **HTTP header** | `Authorization: Bearer <PAT>` per request | `Cookie: PLAY_SESSION=...` per request, obtained via `POST /logIn` first |
| **Credential lifetime** | Long-lived (months/years) | Short-lived (session TTL), needs refresh |
| **Where credentials live** | One PAT per integration, stored in env vars or secret manager | Username/password (or SSO/OIDC flow) per user identity |
| **Audit story** | "Token X performed Y" | "User `jianmin` performed Y at 09:14 EST" |
| **Service-account question** | Easy: cut a dedicated PAT | Harder: who owns the service account password? |
| **Common in** | Greenfield labs, small teams, lab environments | Enterprises with SSO, compliance-driven shops, regulated industries |

Lab and homegrown DataHub deployments typically pick token auth because it's simple. **Enterprises pick session auth because it ties every action to a real SSO identity that can be audited, MFA-enforced, and revoked the moment someone leaves.**

---

## 2. The Problem: Session Auth Is Hostile to Scripts

Session-based auth is natural for humans browsing the DataHub UI — log in once, use the app. But for **programmatic clients** (ingestion scripts, assertion publishers, ETL jobs that push metadata), it creates friction:

- Every script needs a login flow before every batch
- Sessions expire mid-job; scripts need retry-on-401 logic
- Username/password sits in script config — credential sprawl, audit gaps, no easy rotation
- SSO redirect flows (OIDC, SAML) don't make sense from a headless script

The naive solution — "just give the script a username and password" — defeats the security model that made SSO worth adopting in the first place.

---

## 3. The Auth Proxy Pattern

The solution adopted in enterprise DataHub deployments: **introduce a local Auth Proxy on every client host**.

```
Application code  →  http://127.0.0.1:8080/openapi/v3/...  (no auth header)
                              ↓
                     Auth Proxy (on same host)
                              ↓
                     adds Cookie: PLAY_SESSION=...
                     (obtained via SSO/OIDC flow, refreshed automatically)
                              ↓
                     https://datahub.gms.firm.com/openapi/v3/...
```

The application talks plain HTTP to `localhost`. The proxy handles all the auth complexity — login, cookie management, token refresh, MFA token caching, retries on session expiry. The application never sees a credential.

### Similar patterns in the wild

This is a well-established design with many parallels:

| Pattern | Domain |
|---|---|
| **HashiCorp Vault Agent** | Sidecar holds credentials, app calls localhost |
| **`gcloud` / `aws-cli` credential helpers** | SDK libraries delegate auth to the local CLI |
| **SPIFFE/SPIRE workload identity agents** | Service mesh credential delivery |
| **Kerberos `kinit` ticket cache** | The historical equivalent |
| **Docker credential helpers** | Same pattern for registry auth |

The shape is recognizable across decades: **separate the credential-handling concern from the application concern, and put them in different processes on the same trust boundary.**

---

## 4. Benefits of the Auth Proxy

| Benefit | What it solves |
|---|---|
| **Client code stays auth-naive** | Application just does `POST http://localhost:8080/openapi/v3/...` — no login flow, no cookie management, no retry-on-401 logic in business code |
| **Credentials isolated** | Username/password (or refresh token, or service account key) lives in the proxy's config — never in app code, app logs, or process env vars |
| **SSO/refresh handled centrally** | One auth flow implementation, with proper refresh-token rotation, MFA handling, session renewal — done once, used by every script |
| **Audit + observability** | Every request logged with the resolved user identity. Compliance loves this. |
| **Defense in depth** | App compromise alone doesn't yield DataHub access — attacker also needs to compromise the proxy or its config |
| **Rotate without redeploy** | Change credentials in the proxy; apps using it never restart |

---

## 5. Trade-offs to Accept

| Trade-off | Mitigation |
|---|---|
| **Operational footprint** — one more thing to deploy + monitor on every client host | Usually a tiny supervised process (systemd unit, Windows service); monitoring piggybacks on existing host monitoring |
| **Single point of failure per host** — proxy dies → all scripts on that host break | Health-check the proxy; auto-restart; failure is local-only, doesn't cascade |
| **Local-only by design** — proxy listens on `127.0.0.1` so credentials never travel beyond TLS to the proxy | This is intentional — feature, not bug |
| **Initial setup cost** — every team has to know the proxy exists, configure it, plumb its endpoint | Provide a standard team starter kit and "how to onboard" doc |

---

## 6. Architectural Consequence: Push-Only Metadata Ingestion

The Auth Proxy choice **eliminates DataHub's centralized "Data Source" crawler pattern**. This isn't an explicit decision — it's an automatic consequence of the credential model.

### Why centralized crawlers stop working

DataHub's built-in ingestion framework assumes:

- DataHub holds connection strings + credentials to every source database
- DataHub itself reaches OUT to crawl each database on a schedule
- One privileged service has read access to every system in the firm

That model is **fundamentally incompatible with SSO-only auth.** There is no SSO identity DataHub can impersonate to query an arbitrary application's Postgres or Snowflake. Even if you create service accounts as a workaround, you've reintroduced credential sprawl in the very place SSO was supposed to eliminate it.

When the Auth Proxy enforces "credentials live with the application, never with the catalog," the centralized crawler ceases to be a viable pattern.

### What replaces it: push from every client

```
Application team A         Application team B          Application team C
       │                          │                          │
       │ owns their DB            │ owns their DB            │ owns their DB
       │ owns their creds         │ owns their creds         │ owns their creds
       │                          │                          │
       └── pushes metadata ──┐    └── pushes metadata ──┐    └── pushes metadata ──┐
                             │                          │                          │
                             ▼                          ▼                          ▼
                                       DataHub
                              (pure metadata receiver — owns nothing,
                                receives everything, presents it)
```

Every team is responsible for pushing:

- **Data catalog metadata** (datasets, schemas, tables, fields)
- **Lineage** (column-level where possible, dataset-level minimum)
- **Assertions** (DQ checks each team runs)
- **Assertion run events** (day-to-day status, pass/fail, actual values)
- **Operational stats** (row counts, last refresh time, partition health)

DataHub does no crawling. It is a **pure metadata receiver**.

---

## 7. Why This Aligns with Data Mesh

The push-from-client model is a textbook example of **data mesh principles applied to metadata**:

| Centralized crawler | Push-from-client |
|---|---|
| Central team owns "what's in the catalog" | Domain teams own their own metadata |
| Catalog gets stale if crawler config drifts | Catalog is as fresh as the team's last push |
| One team must understand every system | Each team knows their own system best |
| DataHub holds credentials to everything | DataHub holds credentials to nothing |
| One service to compromise → catastrophic blast radius | Compromise is contained to one team's surface |

The Auth Proxy isn't *just* a security pattern. It's an enforcement mechanism for a particular organizational model: **data and metadata are owned by domain teams, not by a central catalog team.**

---

## 8. Trade-offs of Push-Only Ingestion (and how enterprises manage them)

| Pain | Mitigation typical at scale |
|---|---|
| Every team has to write/own ingestion code | Platform team publishes a "blessed" SDK or template — teams import it, don't reinvent |
| Inconsistency: team A pushes 5 fields, team B pushes 50 | A **thin standard** (e.g., mandatory check names like SLA_VALIDATION, RECORD_COUNT) enforced via policy |
| Teams might forget / stop pushing | A "freshness SLA" on metadata; alert if a dataset hasn't pushed in N days |
| "Shadow systems" not in the catalog | Hard to solve technically — needs governance pressure ("if it's not in DataHub, it doesn't exist for compliance/audit") |
| Higher onboarding cost per team | Internal docs + a starter kit, owned by the data platform team |

The "thin standard" pattern deserves special attention — it's how you preserve enough consistency to make the catalog useful across teams without dictating implementation details. Each producer is free to publish however they like *as long as* a small set of well-known signals are always present.

---

## 9. Implications for Application Code

The big practical point: **the Auth Proxy doesn't change the application's architecture, only its base URL.**

A producer application that publishes assertions to DataHub via OpenAPI v3 looks the same with or without an Auth Proxy. The only difference is `gms_url`:

```python
# Without Auth Proxy (lab/dev — direct PAT auth)
gms_url = "https://datahub.firm.com:8080"
headers = {"Authorization": f"Bearer {token}"}

# With Auth Proxy (production)
gms_url = "http://127.0.0.1:8080"
headers = {}  # proxy adds the right auth header
```

Everything else — the payload shape, the endpoint paths, the assertion model, the lineage emission — is **identical**. The decoupled producer/consumer architecture (producers DataHub-only, consumers map downstream) works the same way in both worlds.

This is why a lab built without an Auth Proxy can still produce code that runs at work: only the configuration changes, not the design.

---

## 10. The Pattern in One Sentence

> **The Auth Proxy delegates credential handling to a local sidecar, which automatically forces a push-only metadata model and aligns DataHub deployment with data mesh principles — at the cost of one extra process per client host.**

---

## 11. Quick Reference

```
WHEN TO USE THE AUTH PROXY PATTERN
  └── Enterprise DataHub installation behind SSO
  └── Compliance requires every action tied to an SSO identity
  └── No PAT auth available (or explicitly disabled)

WHAT THE PROXY DOES
  └── Listens on 127.0.0.1:<port>
  └── Forwards requests to DataHub GMS
  └── Handles SSO login, session refresh, cookie/header injection

WHAT THE APPLICATION DOES
  └── Talks to http://127.0.0.1:<port> with NO auth header
  └── Pushes catalog + lineage + assertions + runtime stats
  └── Owns its own data and database credentials

WHAT DATAHUB DOES
  └── Receives metadata, stores it, presents it
  └── Holds NO credentials to any source database
  └── Performs NO centralized crawling
```

---

*Companion to [Applications_to_DataHub_to_DF360_Integration_patterns.md](../../dataflow_360_claude/datahub_to_df360/docs/Applications_to_DataHub_to_DF360_Integration_patterns.md) — the decoupled producer/consumer model documented there is the same model the Auth Proxy pattern enforces.*
