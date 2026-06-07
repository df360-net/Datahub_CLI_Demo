# datahub_proxy_app1

A DataHub integration pattern using auth proxy: a daily ETL that
publishes **all** its metadata to DataHub through a local **Auth Proxy**, never
touching DataHub directly. Two components live in this repo:

1. **Auth Proxy** (`proxy/`) — a loopback-only sidecar that holds DataHub's SSO
   session cookies so application code stays auth-naive. The app talks plain
   HTTP to `localhost:8080`; the proxy injects the session and forwards to
   DataHub.
2. **CardCompass** (`cardcompass/`) — a 3-layer credit-card ETL
   (stage → integration → report) that publishes catalog, lineage, and data
   quality assertions to DataHub **only through the proxy**.

The design rationale lives in the companion docs:
[docs/DataHub_Auth_Proxy_Pattern.md](docs/DataHub_Auth_Proxy_Pattern.md) (why),
[docs/Auth_Proxy_Design.md](docs/Auth_Proxy_Design.md) (the proxy),
[docs/Sample_ETL_Application_design.md](docs/Sample_ETL_Application_design.md) (the app).

---

## How it fits together

```
  daily CSV files                CardCompass ETL                Auth Proxy            DataHub
  card_auth_*.csv  ──load──►  CARD_STG ─► CARD_INT ─► CARD_RPT      │       (192.168.0.16:9002)
  card_post_*.csv                  │  (MSSQL DCF_DB          )      │
                                   │                                │
                                   └── catalog + lineage +  ──────► 127.0.0.1:8080 ──► session cookie
                                       43+ assertion runs           (no auth header)    injected, forwarded
```

- **Push-only:** the app pushes metadata; DataHub crawls nothing and holds no
  credentials. Enforced by the proxy.
- **Decoupled producer:** payloads use DataHub-native concepts only
  (`customAssertion.type`, generic `source.*` properties). No downstream
  (`df360.*`) knowledge.
- **PID-namespaced URNs:** every URN carries the firm Enterprise PID as
  DataHub's `platformInstance`, e.g.
  `urn:li:dataset:(urn:li:dataPlatform:mssql,CARDC.DCF_DB.CARD_STG.card_auth,PROD)`.
- **Status-checked everywhere:** every API call's status is checked; failures
  are surfaced, never swallowed.

---

## Prerequisites

- **Python 3.11+** with a virtualenv.
- **Microsoft ODBC Driver 18 for SQL Server** on the machine running the ETL
  (`winget install --id Microsoft.msodbcsql.18`).
- Network reach to **Server** (`192.168.0.16`): DataHub frontend on `:9002`,
  MSSQL on `:1433` (the firewall must allow `1433` inbound).
- DataHub login credentials and the `DCFDBUSR` MSSQL password.

---

## Setup

```bash
# 1. Install dependencies (workplace-pinned stack + dev/test tools)
pip install -r requirements.txt -r requirements-dev.txt

# 2. Configure secrets — copy the template and fill in the blanks
cp .env.example .env
#    Set at least: DATAHUB_USER, DATAHUB_PW, MSSQL_PASSWORD, ENTERPRISE_PID
#    .env is gitignored; never commit it.

# 3. Create the database objects on Murphy (run as `sa`, once).
#    DDL is the DBA's job; DCFDBUSR does DML only.
sqlcmd -S 192.168.0.16,1433 -U sa -P <sa-pw> -C -i db/00_create_user.sql
sqlcmd -S 192.168.0.16,1433 -U sa -P <sa-pw> -C -d DCF_DB -i db/01_init.sql
sqlcmd -S 192.168.0.16,1433 -U sa -P <sa-pw> -C -d DCF_DB -i db/02_seed_reference.sql
```

`00_create_user.sql` creates the login, 4 schemas, and DML grants;
`01_init.sql` creates the 9 tables; `02_seed_reference.sql` loads the reference
data (100 MCCs, 100 merchants, 200 cards).

### Sanity checks

```bash
# DCFDBUSR can connect + do DML
sqlcmd -S 192.168.0.16,1433 -U DCFDBUSR -P <pw> -C -d DCF_DB -Q "SELECT USER_NAME()"

# DataHub session login returns the cookies
curl -i -X POST http://192.168.0.16:9002/logIn \
  -H "Content-Type: application/json" \
  -d '{"username":"<user>","password":"<pw>"}'
# expect: 200 with Set-Cookie: PLAY_SESSION=... and actor=...
```

---

## Running the daily load

The proxy must be running first — the ETL refuses to publish without it.

```bash
# Terminal 1 — start the Auth Proxy (foreground, 127.0.0.1:8080)
python -m proxy

# health check
curl http://127.0.0.1:8080/proxy/healthz
```

```bash
# Terminal 2 — generate mock files for a date, then run the ETL
python tools/generate_daily_file.py --date 2026-06-07 --rows 500
python -m cardcompass.daily_load --date 2026-06-07
```

`daily_load` (defaults to *yesterday* if `--date` is omitted) runs the whole
pipeline under a load lock and prints a one-line summary:

```
daily_load OK date=2026-06-07 stage={'card_auth': 501, 'card_post': 458} \
  integration={'card_txn': 498, 'card_txn_decline': 29} \
  report={'daily_card_summary': 36, 'card_top_merchants': 10} catalog=6 lineage=6
```

It exits non-zero (and stops) if the proxy is down, a DB step fails, or another
run holds the lock. Re-running the same date is idempotent.

### What lands in DataHub

Open http://192.168.0.16:9002 and search `DCF_DB`. For each of the 6 datasets:

- **Columns / Properties** — schema + `source.*` properties
- **Lineage** — the stage → integration → report flow, with column-level edges
  (click "Show Column Lineage")
- **Quality** — the firm-mandatory + custom assertion runs with pass/fail and
  actual values (the "Category" column is `customAssertion.type`)

---

## What the daily load publishes

Every `daily_load` run pushes four kinds of metadata to DataHub, all through the
proxy. Three are **reference metadata** (versioned, idempotent); one is
**timeseries** (accumulates).

| Published each run | DataHub aspect | Kind | CDC behavior |
|---|---|---|---|
| Schema / catalog metadata | `datasetProperties`, `schemaMetadata` | versioned | **Idempotent** — re-publishing identical content is a **no-op**: no new version, no MetadataChangeLog (MCL) |
| Lineage (table + column) | `upstreamLineage` | versioned | **Idempotent** no-op when unchanged |
| Assertion definitions | `assertionInfo` | versioned | **Idempotent** no-op — byte-stable run to run; carries no run-time timestamp, so it changes only when an assertion is added or modified |
| Assertion runtime stats | `assertionRunEvent` | timeseries | **Appends** a new pass/fail datapoint every run — the day-over-day DQ history |

What this means from a **CDC perspective**:

- **The metadata catalog is idempotent.** DataHub dedupes versioned aspects, so
  an unchanged daily re-publish emits **no change event** on the MCL/CDC stream
  that downstream consumers subscribe to. Only a real change (a new column, a new
  lineage edge, a new or edited assertion definition) propagates downstream.
- **Only the runtime stats accumulate.** `assertionRunEvent` is never deduped —
  each run appends one timestamped result per assertion, building the trend shown
  in the Quality tab. The definition it attaches to stays put.
- This is why re-running a date is safe and quiet: catalog, lineage, and
  assertion definitions converge to one stable copy per URN, while the run events
  simply gain one more datapoint.

So a single `daily_load` keeps the full catalog picture fresh (without churning
the CDC stream) and records that day's data-quality signals.

---

## Tests

```bash
pytest                                   # full unit suite (no Murphy needed)
pytest --cov=proxy --cov=cardcompass     # with coverage
pytest tests/test_session.py             # one module
```

The suite uses a mock upstream, so the proxy (login, 401 recovery, header
handling) and the ETL helpers are tested without touching Murphy.

---

## Layout

```
proxy/            Auth Proxy: config, server, upstream, auth/ (session), health
cardcompass/      ETL: stage, integration, report, catalog, lineage, urn,
                  emit, daily_load (orchestrator), assertions/ (specs, publisher)
db/               SQL run by sa: 00_create_user, 01_init, 02_seed_reference
tools/            generate_daily_file.py (mock feed generator)
tests/            pytest suite + mock-upstream harness
docs/             design docs (pattern, proxy, ETL)
README.md         this overview
.env.example      documented config template (copy to .env)
```

See the design docs in [docs/](docs/) for the full rationale and the design
decisions behind this project.
