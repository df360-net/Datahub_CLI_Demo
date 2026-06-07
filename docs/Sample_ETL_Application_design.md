# Sample ETL Application — Design (Draft v2)

This document specifies the **sample ETL application** that sits on top of the Auth Proxy. The application is the actual reason this project exists — the proxy is plumbing. This is the workload that demonstrates the full push-only DataHub integration pattern from end to end.

**Status: revised draft after Jianmin's v2 feedback.** Confirmed in v1 + v2 review:
- **Application name:** **CardCompass** (Python package `cardcompass`, `SOURCE_APP_NAME=CardCompass`)
- **Database:** Microsoft SQL Server — DB named **`DCF_DB`** (matches workplace), user **`DCFDBUSR`** (matches workplace), already installed on Murphy
- **Schemas:** 4 total — `CARD_STG`, `CARD_INT`, `CARD_RPT`, `CARD_REF` (CARD_-prefixed because DCF_DB hosts many apps)
- **Tables:** 6 total — 2 stage / 2 integration / 2 report (plus 3 reference tables in CARD_REF, pre-loaded)
- **Lineage:** dataset-level AND column-level in v1
- **Daily volume:** 500 rows/day
- **Python stack:** aligned with Jianmin's workplace `requirements.txt` — `acryl-datahub`, `requests`, `urllib3`, `pyodbc`, `pydantic`, `sqlalchemy`, `python-dotenv` (no `httpx`, no `pydantic-settings`). See repo-root `requirements.txt`.

Architectural principles still inherit from [`DataHub_Auth_Proxy_Pattern.md`](DataHub_Auth_Proxy_Pattern.md).

---

## 1. Goals and Non-Goals

### Goals

| | Why |
|---|---|
| **Realistic 3-layer ETL** (stage → integration → report) | Mirrors a typical enterprise data pipeline shape |
| **Daily load cadence** with a single-command runner | Operational shape matches how real ETLs run |
| **Pushes ALL metadata through the Auth Proxy** (catalog, lineage, assertions, assertion runs) | The whole point of the rehearsal |
| **Source data is mock-generated** (no external dependencies) | Lab must be reproducible without internet, VPN, or paid feeds |
| **5 firm-mandatory dataset assertions per published dataset** | Honors the thin-standard contract from [`Applications_to_DataHub_to_DF360_Integration_patterns.md`](../../dataflow_360_claude/datahub_to_df360/docs/Applications_to_DataHub_to_DF360_Integration_patterns.md) |
| **A few custom column-level assertions** | Demonstrates that mandatory + free-form coexist |
| **Single, opinionated tech stack** — Python + Microsoft SQL Server | Match Jianmin's work environment; reuse Murphy's existing MSSQL |
| **Dataset AND column-level lineage** | Richer demo of DataHub's lineage capability |
| **Idempotent daily loads** | Re-running a date should produce identical results |
| **Status-checked at every layer** | Per [`Auth_Proxy_Design.md`](Auth_Proxy_Design.md) §4 — never dump-and-forget |

### Non-Goals

| | Why not |
|---|---|
| Production-grade error recovery (resumable mid-flight, automatic alerts) | Lab rehearsal, not production. Keep it simple. |
| Streaming or near-real-time ingestion | Batch daily mirrors the typical enterprise pattern |
| External orchestrator (Airflow, Prefect, Dagster) | Adds complexity that distracts from the integration pattern. Plain Python entry point + manual or cron trigger is sufficient. |
| Real-time DQ alerting | DataHub gets the assertion runs; that's enough for v1 |

---

## 2. Application Overview

The sample application is **CardCompass** (Python package `cardcompass`, `SOURCE_APP_NAME=CardCompass`) — a daily ETL that processes credit card transactions for a small payments business.

**Why card transactions:**

- Universally understood domain (sales, banking, retail all touch this)
- Distinct from the existing `datahub_app1` (which is loan applications) — no confusion about which is which
- Two-feed model (auth + settlement) is the real banking pattern and gives interesting lineage
- Rich enough to exercise all 5 firm-mandatory assertions naturally (SLA, count, business-date, threshold, duplicate)

**The daily story:**

1. Early each morning, the payment processor drops **two files** for the previous business day:
   - `/data/inbound/card_auth_<YYYYMMDD>.csv` — every authorization attempt (approved + declined)
   - `/data/inbound/card_post_<YYYYMMDD>.csv` — every settlement / posting record (subset of approved auths that cleared)
2. The ETL picks up both files, lands them in the **stage** layer as two separate tables.
3. It cleans, joins (auth ⋈ post ⋈ reference), and produces the **integration** layer: one row per real transaction, plus a separate decline table.
4. It aggregates by category, merchant, and time window to produce the **report** layer.
5. Along the way, the application **publishes catalog metadata, table + column lineage, and assertion runs** to DataHub via the local Auth Proxy — never directly.

If yesterday's run never happened or partially failed, today's run can replay it idempotently.

---

## 3. Data Model

Six tables across three layers. MSSQL types throughout.

### 3.1 Stage layer — `CARD_STG` schema

Two tables, mirror the two inbound files. Everything is `nvarchar` — no type coercion at this layer.

**`CARD_STG.card_auth`** — raw landing of the authorization file

| Column | Type | Notes |
|---|---|---|
| `txn_id` | nvarchar(40) | PK in the source file |
| `card_number_hash` | nvarchar(64) | SHA-256 of the PAN (source pre-hashes; we never see real card numbers) |
| `auth_ts` | nvarchar(32) | ISO-8601 timestamp string |
| `auth_amount` | nvarchar(20) | Decimal as string |
| `txn_currency` | nvarchar(3) | ISO 4217 code, e.g., `USD` |
| `merchant_id` | nvarchar(20) | Source merchant identifier |
| `mcc` | nvarchar(4) | Merchant category code |
| `txn_type` | nvarchar(20) | `PURCHASE` / `REFUND` / `AUTH_ONLY` |
| `response_code` | nvarchar(4) | `00`=approved, others=declined |
| `file_seq` | nvarchar(20) | Source row sequence — useful for debugging |
| `load_business_date` | date | The business date this file is for |
| `loaded_at` | datetime2(3) | When the row was inserted into stage |

**`CARD_STG.card_post`** — raw landing of the settlement/posting file

| Column | Type | Notes |
|---|---|---|
| `txn_id` | nvarchar(40) | FK to the auth's `txn_id` |
| `posting_ref` | nvarchar(40) | PK in the source file |
| `posting_ts` | nvarchar(32) | When the transaction was posted to the cardholder's account |
| `settled_amount` | nvarchar(20) | Decimal as string — may differ slightly from auth (tip, FX) |
| `settlement_currency` | nvarchar(3) | |
| `acquirer_id` | nvarchar(20) | Settling bank |
| `file_seq` | nvarchar(20) | |
| `load_business_date` | date | |
| `loaded_at` | datetime2(3) | |

### 3.2 Integration layer — `CARD_INT` schema

Cleaned, typed, deduped, enriched, auth+post joined.

**`CARD_INT.card_txn`** — one row per real card transaction

| Column | Type | Notes | Sourced from |
|---|---|---|---|
| `txn_id` | nvarchar(40) | PK | `CARD_STG.card_auth.txn_id` |
| `card_id` | bigint | FK into `CARD_REF.card` | resolved from `CARD_STG.card_auth.card_number_hash` |
| `auth_ts` | datetime2(3) | Typed | `CARD_STG.card_auth.auth_ts` |
| `posting_ts` | datetime2(3) | NULL if not yet posted (declines, recent auths) | `CARD_STG.card_post.posting_ts` |
| `txn_date` | date | Business date | derived |
| `auth_amount` | decimal(18,2) | Typed | `CARD_STG.card_auth.auth_amount` |
| `settled_amount` | decimal(18,2) | NULL if not posted | `CARD_STG.card_post.settled_amount` |
| `txn_currency` | char(3) | | `CARD_STG.card_auth.txn_currency` |
| `merchant_id` | nvarchar(20) | | `CARD_STG.card_auth.merchant_id` |
| `merchant_name` | nvarchar(100) | Enriched | `CARD_REF.merchant.merchant_name` |
| `mcc` | char(4) | | `CARD_STG.card_auth.mcc` |
| `mcc_category` | nvarchar(50) | Enriched | `CARD_REF.mcc_category.mcc_category` |
| `txn_type` | nvarchar(20) | | `CARD_STG.card_auth.txn_type` |
| `is_approved` | bit | Derived from `response_code = '00'` | `CARD_STG.card_auth.response_code` |
| `is_posted` | bit | True if matched in `CARD_STG.card_post` | derived from join |
| `decline_reason` | nvarchar(100) | NULL if approved | `CARD_STG.card_auth.response_code` (mapped) |
| `loaded_at` | datetime2(3) | | |

**`CARD_INT.card_txn_decline`** — separate physical table for fraud / risk consumers

| Column | Type | Notes | Sourced from |
|---|---|---|---|
| `txn_id` | nvarchar(40) | PK, FK to `CARD_INT.card_txn.txn_id` | |
| `card_id` | bigint | | `CARD_INT.card_txn.card_id` |
| `txn_date` | date | | `CARD_INT.card_txn.txn_date` |
| `auth_amount` | decimal(18,2) | | `CARD_INT.card_txn.auth_amount` |
| `decline_reason` | nvarchar(100) | | `CARD_INT.card_txn.decline_reason` |
| `flagged_for_review` | bit | True if matches simple fraud heuristics | derived |
| `loaded_at` | datetime2(3) | | |

### 3.3 Report layer — `CARD_RPT` schema

Aggregated, business-facing.

**`CARD_RPT.daily_card_summary`** — one row per (txn_date, mcc_category, txn_type)

| Column | Type | Notes | Sourced from |
|---|---|---|---|
| `txn_date` | date | | `CARD_INT.card_txn.txn_date` |
| `mcc_category` | nvarchar(50) | | `CARD_INT.card_txn.mcc_category` |
| `txn_type` | nvarchar(20) | | `CARD_INT.card_txn.txn_type` |
| `txn_count` | int | Total transactions in slice | `COUNT(*)` of `CARD_INT.card_txn` |
| `approved_count` | int | Subset approved | derived from `CARD_INT.card_txn.is_approved` |
| `decline_count` | int | Subset declined | derived from `CARD_INT.card_txn.is_approved` |
| `auth_amount_total` | decimal(18,2) | Sum of approved auth amounts | `SUM(CARD_INT.card_txn.auth_amount)` |
| `settled_amount_total` | decimal(18,2) | Sum of settled amounts (approved only) | `SUM(CARD_INT.card_txn.settled_amount)` |
| `loaded_at` | datetime2(3) | | |

Composite PK: `(txn_date, mcc_category, txn_type)`.

**`CARD_RPT.card_top_merchants`** — top 10 merchants per day by approved settled volume

| Column | Type | Notes | Sourced from |
|---|---|---|---|
| `txn_date` | date | | `CARD_INT.card_txn.txn_date` |
| `rank` | int | 1..10 | `ROW_NUMBER()` derived |
| `merchant_id` | nvarchar(20) | | `CARD_INT.card_txn.merchant_id` |
| `merchant_name` | nvarchar(100) | | `CARD_INT.card_txn.merchant_name` |
| `txn_count` | int | | `COUNT(*)` |
| `settled_amount_total` | decimal(18,2) | | `SUM(CARD_INT.card_txn.settled_amount)` |
| `loaded_at` | datetime2(3) | | |

Composite PK: `(txn_date, rank)`.

### 3.4 Reference data — `CARD_REF` schema (pre-loaded, not part of the daily flow)

Static / slowly-changing lookups. Pre-loaded by a one-shot seed script. Joined into integration.

| Table | Purpose |
|---|---|
| `CARD_REF.card` | Card master — `card_id` (PK), `card_number_hash` (unique), `customer_id`, `issue_date`, `is_active` |
| `CARD_REF.merchant` | Merchant master — `merchant_id` (PK), `merchant_name`, `mcc`, `country` |
| `CARD_REF.mcc_category` | MCC → human category — `mcc` (PK), `mcc_category` |

---

## 4. Source Data (Mock Generator)

Since the lab can't depend on a real payment processor, we ship a **mock data generator** that produces **both files** for a given date — auths and the corresponding postings.

**`tools/generate_daily_file.py`**

```
python tools/generate_daily_file.py --date 2026-06-07 --rows 500
→ writes /data/inbound/card_auth_20260607.csv  (500 rows)
→ writes /data/inbound/card_post_20260607.csv  (~470 rows — ~99% of approved auths post)
```

Generator characteristics:

- Deterministic for a given `(date, --seed)` — same inputs → identical files (idempotent demo)
- Row count configurable (default 500)
- ~5% of auth rows are declines with realistic reason codes (declines never post)
- ~1% intentionally-malformed rows (negative amounts, future dates, invalid MCC) so the assertions have something to catch
- ~0.1% intentional duplicates (same `txn_id` repeated) so `DUPLICATE_FEED` has something to fire on occasionally
- Posting file omits declines and a small late-settling subset of approved auths (~2% straggle to the next day)
- Card hashes pulled from the pre-loaded `CARD_REF.card` table (200 cards by default), so the integration join populates
- Merchant IDs pulled from `CARD_REF.merchant` (100 merchants by default)

The generator runs separately from the ETL — it's a lab convenience. Real deployments would have an external system drop the files in `/data/inbound/`.

---

## 5. Layer Walkthroughs

### 5.1 Stage load (two tables)

Inputs: `/data/inbound/card_auth_<YYYYMMDD>.csv`, `/data/inbound/card_post_<YYYYMMDD>.csv`.

Steps (for each file, independently):

1. Locate the file. If missing → fail loudly.
2. **Idempotent reset**: `DELETE FROM CARD_STG.card_auth WHERE load_business_date = :biz_date` (and analogously for `CARD_STG.card_post`).
3. Stream the CSV row by row, inserting via `pyodbc.fast_executemany`. All columns stored as `nvarchar` (type validation happens at integration).
4. `COMMIT` at end-of-file. On any error, the transaction rolls back and the date's stage rows remain untouched.
5. Record row count, file checksum, and load duration for assertion context.

### 5.2 Integration load

Input: `CARD_STG.card_auth` + `CARD_STG.card_post` for the business date. Output: rows in `CARD_INT.card_txn` + `CARD_INT.card_txn_decline`.

Steps:

1. `DELETE FROM CARD_INT.card_txn WHERE txn_date = :biz_date` (and similar for `CARD_INT.card_txn_decline`).
2. INSERT via `MERGE` or `INSERT ... SELECT` joining stage auth with `LEFT JOIN` posting and ref tables:
   ```
   INSERT INTO CARD_INT.card_txn (...)
   SELECT
       a.txn_id,
       c.card_id,
       TRY_CAST(a.auth_ts AS datetime2) AS auth_ts,
       TRY_CAST(p.posting_ts AS datetime2) AS posting_ts,
       ...
   FROM   CARD_STG.card_auth a
   JOIN   CARD_REF.card      c  ON c.card_number_hash = a.card_number_hash
   JOIN   CARD_REF.merchant  m  ON m.merchant_id      = a.merchant_id
   JOIN   CARD_REF.mcc_category mc ON mc.mcc          = a.mcc
   LEFT JOIN CARD_STG.card_post p ON p.txn_id         = a.txn_id
                            AND p.load_business_date = a.load_business_date
   WHERE  a.load_business_date = :biz_date
     AND  <data validity filters>
   ```
3. The `<data validity filters>` exclude malformed rows. Excluded counts surface via assertion `actual_value`.
4. Populate `CARD_INT.card_txn_decline` from the declined subset of the freshly-inserted `CARD_INT.card_txn`.
5. `COMMIT`.

### 5.3 Report load

Input: `CARD_INT.card_txn` for the business date. Output: rows in `CARD_RPT.daily_card_summary` + `CARD_RPT.card_top_merchants`.

Steps:

1. `DELETE FROM CARD_RPT.daily_card_summary WHERE txn_date = :biz_date` (and similar for top merchants).
2. `INSERT INTO CARD_RPT.daily_card_summary (...) SELECT ... GROUP BY txn_date, mcc_category, txn_type`.
3. `INSERT INTO CARD_RPT.card_top_merchants (...)` using `TOP 10 ... ORDER BY SUM(settled_amount) DESC` (or `ROW_NUMBER()` partitioned).
4. `COMMIT`.

---

## 6. Lineage

Lineage is published at **two granularities in v1**: dataset-level (table-to-table edges) and column-level (field-to-field edges via `fineGrainedLineages`).

### 6.1 Table-level lineage

```
   card_auth_*.csv                              card_post_*.csv
          │                                            │
          ▼ stage load                                 ▼ stage load
   ┌────────────────────┐                       ┌────────────────────┐
   │ CARD_STG.card_auth │                       │ CARD_STG.card_post │
   └────────────────────┘                       └────────────────────┘
          │   │                                         │
          │   │            ┌────────────────────────────┘
          │   │            │
          │   ▼            ▼   (joined with CARD_REF.card, CARD_REF.merchant, CARD_REF.mcc_category)
          │  ┌─────────────────────┐
          │  │  CARD_INT.card_txn  │
          │  └─────────────────────┘
          │            │      │
          │            │      └────────────────────────────┐
          ▼            │                                   │
   ┌─────────────────────────────┐                         │
   │ CARD_INT.card_txn_decline   │                         │
   └─────────────────────────────┘                         │
                       │                                   │
                       ▼                                   ▼
       ┌─────────────────────────────┐     ┌─────────────────────────────┐
       │ CARD_RPT.daily_card_summary │     │ CARD_RPT.card_top_merchants │
       └─────────────────────────────┘     └─────────────────────────────┘
```

Edges (8 total):

| Downstream | Upstream(s) |
|---|---|
| `CARD_STG.card_auth` | `card_auth_*.csv` (file) |
| `CARD_STG.card_post` | `card_post_*.csv` (file) |
| `CARD_INT.card_txn` | `CARD_STG.card_auth`, `CARD_STG.card_post`, `CARD_REF.card`, `CARD_REF.merchant`, `CARD_REF.mcc_category` |
| `CARD_INT.card_txn_decline` | `CARD_STG.card_auth`, `CARD_INT.card_txn` |
| `CARD_RPT.daily_card_summary` | `CARD_INT.card_txn` |
| `CARD_RPT.card_top_merchants` | `CARD_INT.card_txn` |

### 6.2 Column-level lineage

Published via `fineGrainedLineages` inside each downstream dataset's `upstreamLineage` aspect. Examples per layer (full list lives in `cardcompass/lineage.py`):

**Into `CARD_INT.card_txn`:**

| Downstream column | Upstream column(s) | Transform |
|---|---|---|
| `CARD_INT.card_txn.txn_id` | `CARD_STG.card_auth.txn_id` | identity |
| `CARD_INT.card_txn.card_id` | `CARD_STG.card_auth.card_number_hash`, `CARD_REF.card.card_number_hash`, `CARD_REF.card.card_id` | lookup |
| `CARD_INT.card_txn.auth_ts` | `CARD_STG.card_auth.auth_ts` | TRY_CAST |
| `CARD_INT.card_txn.posting_ts` | `CARD_STG.card_post.posting_ts` | TRY_CAST |
| `CARD_INT.card_txn.auth_amount` | `CARD_STG.card_auth.auth_amount` | TRY_CAST |
| `CARD_INT.card_txn.settled_amount` | `CARD_STG.card_post.settled_amount` | TRY_CAST |
| `CARD_INT.card_txn.mcc_category` | `CARD_STG.card_auth.mcc`, `CARD_REF.mcc_category.mcc_category` | lookup |
| `CARD_INT.card_txn.merchant_name` | `CARD_STG.card_auth.merchant_id`, `CARD_REF.merchant.merchant_name` | lookup |
| `CARD_INT.card_txn.is_approved` | `CARD_STG.card_auth.response_code` | predicate |
| `CARD_INT.card_txn.is_posted` | `CARD_STG.card_post.txn_id` | exists |
| `CARD_INT.card_txn.decline_reason` | `CARD_STG.card_auth.response_code` | code → text map |

**Into `CARD_RPT.daily_card_summary`:**

| Downstream column | Upstream column(s) | Transform |
|---|---|---|
| `CARD_RPT.daily_card_summary.txn_date` | `CARD_INT.card_txn.txn_date` | identity |
| `CARD_RPT.daily_card_summary.mcc_category` | `CARD_INT.card_txn.mcc_category` | identity |
| `CARD_RPT.daily_card_summary.txn_type` | `CARD_INT.card_txn.txn_type` | identity |
| `CARD_RPT.daily_card_summary.txn_count` | `CARD_INT.card_txn.txn_id` | COUNT |
| `CARD_RPT.daily_card_summary.approved_count` | `CARD_INT.card_txn.is_approved` | SUM(CASE) |
| `CARD_RPT.daily_card_summary.decline_count` | `CARD_INT.card_txn.is_approved` | SUM(CASE) |
| `CARD_RPT.daily_card_summary.auth_amount_total` | `CARD_INT.card_txn.auth_amount`, `CARD_INT.card_txn.is_approved` | SUM(filtered) |
| `CARD_RPT.daily_card_summary.settled_amount_total` | `CARD_INT.card_txn.settled_amount`, `CARD_INT.card_txn.is_approved` | SUM(filtered) |

**Into `CARD_RPT.card_top_merchants`:**

| Downstream column | Upstream column(s) | Transform |
|---|---|---|
| `CARD_RPT.card_top_merchants.merchant_id` | `CARD_INT.card_txn.merchant_id` | identity (grouped) |
| `CARD_RPT.card_top_merchants.merchant_name` | `CARD_INT.card_txn.merchant_name` | identity (grouped) |
| `CARD_RPT.card_top_merchants.rank` | `CARD_INT.card_txn.settled_amount` | ROW_NUMBER(ORDER BY SUM) |
| `CARD_RPT.card_top_merchants.settled_amount_total` | `CARD_INT.card_txn.settled_amount` | SUM |

DataHub renders these as field-to-field arrows when you click "Show Column Lineage" on a dataset.

---

## 7. Assertions per Layer

Every published dataset gets the **5 firm-mandatory dataset-level assertions** plus a small set of layer-appropriate custom checks.

### 7.1 Firm-mandatory (every dataset)

| `customAssertion.type` | What it asserts here | Thresholds |
|---|---|---|
| `SLA_VALIDATION` | Latest `loaded_at` timestamp within 24 hours of now | exp_val_2 = "24" (hours) |
| `RECORD_COUNT` | Row count for the business date (informational, always passes) | n/a |
| `BUSINESS_DATE` | All rows have business date ∈ [today-1, today] | n/a |
| `RECORD_COUNT_THRESHOLD` | Daily count between expected min/max | CARD_STG / CARD_INT: 100–1000; CARD_RPT.daily_card_summary: 1–200; CARD_RPT.card_top_merchants: 1–10 |
| `DUPLICATE_FEED` | No duplicate PK rows for the business date | n/a |

These five appear on every one of the **6 published datasets**. Total: **30 dataset-level mandatory assertion runs per daily execution**.

### 7.2 Custom (free-form `customAssertion.type`)

Layer-specific column-level / business-rule checks. Free-form names, routed to `RUL_LVL2_DH_CUSTOM` catch-all on the DF360 side.

**Stage — `CARD_STG.card_auth`:**
- `AUTH_AMOUNT_NUMERIC` — every `auth_amount` parses as a positive decimal
- `AUTH_TS_PARSEABLE` — every `auth_ts` parses as a valid timestamp
- `MCC_FOUR_DIGITS` — `mcc` is exactly 4 digits
- `TXN_TYPE_ENUM` — `txn_type` ∈ {PURCHASE, REFUND, AUTH_ONLY}

**Stage — `CARD_STG.card_post`:**
- `SETTLED_AMOUNT_NUMERIC` — every `settled_amount` parses as a positive decimal
- `POSTING_TS_PARSEABLE` — every `posting_ts` parses as a valid timestamp
- `POST_TXN_ID_IN_AUTH` — every posting `txn_id` exists in the same day's auth feed

**Integration — `CARD_INT.card_txn`:**
- `CARD_ID_RESOLVED` — every row has a non-NULL `card_id` (no orphan transactions)
- `MERCHANT_NAME_PRESENT` — every row has a non-NULL `merchant_name`
- `AUTH_AMOUNT_RANGE` — `auth_amount` ∈ (0, 100000]
- `APPROVED_FLAG_CONSISTENT` — `is_approved` matches `response_code = '00'`
- `POSTED_IMPLIES_APPROVED` — `is_posted = 1` only when `is_approved = 1`

**Integration — `CARD_INT.card_txn_decline`:**
- `DECLINE_REASON_PRESENT` — every row has a non-NULL `decline_reason`

**Report — `CARD_RPT.daily_card_summary`:**
- `APPROVED_PLUS_DECLINE_EQUALS_TXN_COUNT` — internal consistency: `approved_count + decline_count == txn_count`

**Report — `CARD_RPT.card_top_merchants`:**
- `RANK_IS_1_TO_10` — `rank` between 1 and 10, no gaps

Total custom assertion runs per daily execution: **13**. Combined with the 30 mandatory, that's **43 assertion runs published to DataHub every day**.

---

## 8. Daily Orchestrator

Plain Python script. No Airflow, no scheduler library — just a function.

```
python -m cardcompass.daily_load --date 2026-06-07
```

Or, with the date defaulted to "yesterday":

```
python -m cardcompass.daily_load
```

### What it does, in order

1. **Resolve business date** from `--date` or default to yesterday.
2. **Acquire a load lock** (`UPDLOCK, HOLDLOCK` on a row in `etl_locks`) — prevents two runs from clobbering each other.
3. **Stage load** (§5.1): card_auth file → `CARD_STG.card_auth`, card_post file → `CARD_STG.card_post`. On success → publish stage-layer assertions via the proxy.
4. **Integration load** (§5.2). On success → publish integration-layer assertions.
5. **Report load** (§5.3). On success → publish report-layer assertions.
6. **Catalog refresh** — re-publish dataset metadata for all 6 tables (idempotent).
7. **Lineage refresh** — re-publish table-level + column-level lineage (idempotent).
8. **Release lock**. Log a single-line success summary.

If any step fails, the script logs the failure, releases the lock, exits non-zero. The proxy and DataHub state remain consistent because each step is its own transaction.

### Scheduling

Lab: cron (WSL) / Windows Task Scheduler entry to run at, e.g., 06:00 daily. Manual invocation for testing.

---

## 9. DataHub Publishing

Every DataHub call goes through the local proxy at `http://127.0.0.1:8080`. The application code does not know the real DataHub URL or hold any credential.

### What gets published per daily run

| Aspect | When | Endpoint | Idempotent? |
|---|---|---|---|
| **Dataset metadata** (ownership, description) for all 6 datasets | Step 6 | `POST /openapi/v3/entity/dataset` | Yes — upserts by URN |
| **Schema metadata** (column types, descriptions) | Step 6 | `POST /openapi/v3/entity/schemaMetadata` | Yes |
| **Upstream lineage** (table + `fineGrainedLineages` columns) | Step 7 | `POST /openapi/v3/entity/upstreamLineage` | Yes |
| **Assertion entities** (the 43 assertion definitions) | Steps 3, 4, 5 — first time each | `POST /openapi/v3/entity/assertion` | Yes — upserts by URN |
| **Assertion run events** (pass/fail + actual_value) | Steps 3, 4, 5 | `POST /openapi/v3/entity/assertion` (run event appended) | Append-only timeseries — every run is a new event |

The shape of each payload follows the **decoupled producer/consumer model**:

- `customAssertion.type` = one of the 5 firm-mandatory standardized names OR a free-form name
- `customProperties` = generic `source.*` keys only (`source.app`, `source.table`, `source.field` for L2)
- `nativeResults` = generic keys (`actual_value`, `expected_value_1`, `expected_value_2`, `detail_status`, `message`)
- **No `df360.*` keys** anywhere — the application is consumer-unaware

### URN convention

MSSQL URN shape — the firm **Enterprise PID** (`ENTERPRISE_PID`, e.g. `CARDC`)
rides as DataHub's **platformInstance** qualifier so every app's datasets are
unique firm-wide:

```
urn:li:dataset:(urn:li:dataPlatform:mssql,
                <ENTERPRISE_PID>.DCF_DB.<schema>.<table>,
                PROD)
```

`DCF_DB` is the database name; `<ENTERPRISE_PID>` is prepended as the
platform-instance segment. Built via `cardcompass.urn.dataset_urn()` (wraps
`make_dataset_urn_with_platform_instance`). Examples (PID = `CARDC`):

- `urn:li:dataset:(urn:li:dataPlatform:mssql,CARDC.DCF_DB.CARD_STG.card_auth,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:mssql,CARDC.DCF_DB.CARD_STG.card_post,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:mssql,CARDC.DCF_DB.CARD_INT.card_txn,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:mssql,CARDC.DCF_DB.CARD_INT.card_txn_decline,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:mssql,CARDC.DCF_DB.CARD_RPT.daily_card_summary,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:mssql,CARDC.DCF_DB.CARD_RPT.card_top_merchants,PROD)`

Why platformInstance and not a name-embedded prefix: the bare
`mssql,DCF_DB.…` URN contains no server/host, so two apps on different SQL
Servers sharing a `DCF_DB.schema.table` path would collide into one entity.
platformInstance is DataHub's first-class mechanism for exactly this, and the
UI renders it as a proper instance facet while the table name still reads as
the real `DCF_DB.schema.table`.

### Column URN convention (for fine-grained lineage)

```
urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mssql,<ENTERPRISE_PID>.DCF_DB.<schema>.<table>,PROD),<column>)
```

### Publishing patterns

Two approved patterns, both go through the local proxy at `http://127.0.0.1:8080`:

**Option A — `requests` + hand-built payload** (used for the OpenAPI v3 assertion endpoints):

```python
import requests

resp = requests.post(
    f"{PROXY_URL}/openapi/v3/entity/assertion",
    json=[...],
    timeout=30.0,
)
if resp.status_code != 200:
    raise DataHubPublishError(
        f"Publish failed: {resp.status_code} {resp.text[:200]}"
    )
```

**Option B — `acryl-datahub` emitter pointed at the proxy** (used for catalog + lineage where typed aspect classes are easier):

```python
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    UpstreamLineageClass,
    SchemaMetadataClass,
)

emitter = DatahubRestEmitter(gms_server=f"{PROXY_URL}/api/gms", token="")
# /api/gms → the DataHub frontend mounts the Restli API (the SDK's default
#   /aspects endpoint) under that prefix; it 404s at the root. Verified Murphy
#   2026-06-07. OpenAPI calls (Option A) still use PROXY_URL directly.
# token="" → SDK omits Authorization; proxy injects the session cookies

emitter.emit(MetadataChangeProposalWrapper(
    entityUrn="urn:li:dataset:(urn:li:dataPlatform:mssql,DCF_DB.CARD_INT.card_txn,PROD)",
    aspect=DatasetPropertiesClass(name="card_txn", description="..."),
))
# emit() raises on non-2xx — the status-check discipline is built into the SDK
```

The SDK uses `requests` internally, which is why the work stack composes cleanly. No `httpx` anywhere.

### Status-checking discipline

No dump-and-forget. Every `requests.post` checks `resp.status_code`. Every `emitter.emit` is allowed to raise. A publishing failure is loud and fails the orchestrator step.

---

## 10. Database — Microsoft SQL Server

**Choice: MSSQL.** Reasons:

- **Matches Jianmin's work environment** — the firm runs MSSQL; lab patterns transfer directly
- **Already installed on Murphy** (alongside AdventureWorks 2025); no additional installation
- Schemas as namespaces (`CARD_STG`, `CARD_INT`, `CARD_RPT`, `CARD_REF`) work the same as Postgres
- `pyodbc` + ODBC Driver 18 is the firm-standard Python access pattern

**Deployment:** MSSQL on Murphy (`192.168.0.16:1433`). ETL + proxy run on the dev workstation, talk to Murphy over the LAN.

**Database name:** `DCF_DB` (matches workplace). Schemas: `CARD_STG`, `CARD_INT`, `CARD_RPT`, `CARD_REF`.

**Database user:** `DCFDBUSR` (matches workplace) — granted **DML only** (SELECT/INSERT/UPDATE/DELETE/EXECUTE/REFERENCES) on the 4 schemas. DDL (table creation via `db/init.sql`) is performed once by `sa`, not by the app user. This mirrors the workplace's DBA-owns-DDL / app-owns-DML separation.

**Driver:** `pyodbc` with `ODBC Driver 18 for SQL Server`, wrapped by `sqlalchemy` (matches work — `sqlalchemy>=2.0` is in the workplace `requirements.txt`).

SQLAlchemy URL form:

```
mssql+pyodbc://DCFDBUSR:<pw>@192.168.0.16,1433/DCF_DB?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

Engine + Session pattern (SQLAlchemy 2.0 style):

```python
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

engine = create_engine(MSSQL_URL, fast_executemany=True, pool_pre_ping=True)
with Session(engine) as s:
    s.execute(text("DELETE FROM CARD_STG.card_auth WHERE load_business_date = :d"), {"d": biz_date})
    s.commit()
```

Raw `pyodbc.connect(...)` is also fine where the SQLAlchemy layer adds nothing (e.g., bulk insert paths that want `fast_executemany` directly).

**Setup scripts** (all run by `sa`, not by `DCFDBUSR`):
- `db/00_create_user.sql` — creates the `DCFDBUSR` login + user + 4 schemas + DML grants (the script from this design review)
- `db/01_init.sql` — creates the 6 application tables across the 4 schemas
- `db/02_seed_reference.sql` — populates `CARD_REF.card` (200 cards), `CARD_REF.merchant` (100 merchants), `CARD_REF.mcc_category` (~100 MCC categories)

---

## 11. Project Layout

```
datahub_proxy_app1/                           ← separate git repo
├── proxy/                                    ← from Auth_Proxy_Design.md
│   └── ...
├── cardcompass/                             ← the sample ETL application
│   ├── __init__.py
│   ├── __main__.py           # entry point: python -m cardcompass.daily_load
│   ├── config.py             # python-dotenv + pydantic BaseModel for settings
│   ├── db.py                 # sqlalchemy engine + pyodbc driver
│   ├── stage.py              # stage load (handles both files)
│   ├── integration.py        # integration load
│   ├── report.py             # report load
│   ├── catalog.py            # publish dataset + schema metadata (acryl-datahub emitter)
│   ├── lineage.py            # publish table + column-level lineage (acryl-datahub aspects)
│   ├── assertions/
│   │   ├── __init__.py
│   │   ├── specs.py          # CheckSpec list (mandatory + custom)
│   │   ├── runner.py         # executes checks via sqlalchemy, returns CheckResult
│   │   └── publisher.py      # posts assertions + run events via proxy (requests)
│   ├── daily_load.py         # the orchestrator (Section 8)
│   └── urn.py                # dataset + schemaField URN builders (acryl-datahub helpers)
├── db/
│   ├── init.sql              # CREATE DATABASE, schemas, tables
│   └── seed_reference.sql    # 200 cards, 100 merchants, MCC categories
├── tools/
│   └── generate_daily_file.py
├── data/
│   └── inbound/              # daily files land here (gitignored)
├── docs/                     # design + pattern docs (this folder)
├── tests/
│   ├── test_stage.py
│   ├── test_integration.py
│   ├── test_report.py
│   └── test_assertions.py
├── .env.example
├── .gitignore
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

## 12. Configuration

All env vars, optionally loaded from `.env`.

### ETL-specific

| Variable | Required | Default | Description |
|---|---|---|---|
| `MSSQL_HOST` | yes | — | MSSQL host (typically `192.168.0.16` for lab — Murphy) |
| `MSSQL_PORT` | no | `1433` | |
| `MSSQL_DATABASE` | no | `DCF_DB` | |
| `MSSQL_USER` | yes | — | MSSQL user (typically `DCFDBUSR` — matches workplace) |
| `MSSQL_PASSWORD` | yes | — | MSSQL password (DCFDBUSR's, set by the `sa` setup script) |
| `MSSQL_DRIVER` | no | `ODBC Driver 18 for SQL Server` | |
| `INBOUND_DIR` | no | `./data/inbound` | Where to find daily files |
| `PROXY_URL` | no | `http://127.0.0.1:8080` | The Auth Proxy's localhost endpoint |
| `SOURCE_APP_NAME` | no | `CardCompass` | Value stamped on `customProperties["source.app"]` in DataHub payloads |
| `LOG_LEVEL` | no | `INFO` | |

### Proxy env (same `.env`)

Proxy reads its own `DATAHUB_URL`, `DATAHUB_USER`, `DATAHUB_PW`, etc. from the same file. See [`Auth_Proxy_Design.md`](Auth_Proxy_Design.md) §11.

### Dependencies (`requirements.txt`)

Single `requirements.txt` at the repo root; the ETL needs the full list, the proxy needs only a subset. See repo-root `requirements.txt` for the canonical pin set. Quick summary:

| Package | Used by | Role |
|---|---|---|
| `acryl-datahub` | ETL | URN builders, typed aspect classes, `DatahubRestEmitter` (points at proxy) |
| `python-dotenv` | both | `.env` loading |
| `requests` | both | sync HTTP — ETL→proxy, proxy→DataHub |
| `urllib3` | both | retry adapters, lower-level HTTP config |
| `pyodbc` | ETL | MSSQL driver under SQLAlchemy |
| `pydantic` | both | settings / DTO validation (no `pydantic-settings`) |
| `sqlalchemy` | ETL | MSSQL engine/session, query building |

All 7 packages are taken verbatim from Jianmin's workplace `requirements.txt` so lab patterns transfer cleanly to work. The proxy uses **Python's stdlib `http.server.ThreadingHTTPServer`** — no extra framework dependency, matching the workplace pattern.

---

## 13. Testing Strategy

| Test | Mechanism |
|---|---|
| **Generator produces valid auth + post files** | Run generator with a fixed seed + date; assert row counts, column counts, malformed-row percentage |
| **Stage load idempotency** | Run stage twice for the same date; assert row count identical in both stage tables |
| **Auth–post join coverage** | Generate 500 auths + 470 posts; assert ≥95% of approved auths join to a posting in integration |
| **Integration join coverage** | Stage 500 rows; assert ≥99% land in integration (1% expected to fail validity filters) |
| **Report aggregates match integration** | After report load, assert `SUM(CARD_RPT.daily_card_summary.txn_count) = COUNT(CARD_INT.card_txn)` for the date |
| **5 mandatory assertions present on every dataset** | After a daily run, query the proxy's request log; assert each of 6 datasets got all 5 firm assertion types |
| **Column-level lineage emitted per downstream dataset** | After a run, assert `fineGrainedLineages` array is non-empty for `CARD_INT.card_txn`, `CARD_INT.card_txn_decline`, both `CARD_RPT.*` tables |
| **Custom assertion fires on planted bad row** | Inject a row with `auth_amount = -100`; assert `AUTH_AMOUNT_RANGE` returns FAILED with correct `actual_value` |
| **Status check fails loudly** | Mock the proxy to return 500; assert the daily orchestrator exits non-zero with a clear error |
| **Daily load reruns cleanly** | Run for date D twice in a row; assert final DB state and DataHub publishes are identical |
| **Proxy down during run** | Stop the proxy; run daily load; assert ETL fails fast at the first publish attempt with a clear error |

---

## 14. What "Done" Looks Like for v1

A single command:

```
python -m cardcompass.daily_load --date 2026-06-07
```

…that:

1. Reads `/data/inbound/card_auth_20260607.csv` and `/data/inbound/card_post_20260607.csv` (generated by `tools/generate_daily_file.py`)
2. Lands 500 rows in `CARD_STG.card_auth` and ~470 rows in `CARD_STG.card_post` for that date
3. Produces ~495 rows in `CARD_INT.card_txn` (excluded malformed ones; declines have NULL posting)
4. Produces ~25 rows in `CARD_INT.card_txn_decline`
5. Produces ~15-20 rows in `CARD_RPT.daily_card_summary`
6. Produces 10 rows in `CARD_RPT.card_top_merchants`
7. Publishes 6 dataset entities + their schemas to DataHub via the proxy
8. Publishes 6 upstream-lineage aspects (one per downstream dataset, each with table edges AND `fineGrainedLineages` column edges) via the proxy
9. Publishes 43 assertion runs to DataHub via the proxy (30 mandatory + 13 custom)
10. Logs a one-line success summary and exits 0

And these can be verified:

- DataHub UI at http://192.168.0.16:9002 shows the 6 `DCF_DB.CARD_*` datasets with their schemas, all tagged with `source.app = CardCompass`
- Lineage graph in the UI shows the auth/post files → stage → integration → report flow
- Clicking "Show Column Lineage" on `CARD_RPT.daily_card_summary` traces `auth_amount_total` back through `CARD_INT.card_txn.auth_amount` to `CARD_STG.card_auth.auth_amount`
- Each dataset's "Validations" tab shows the latest assertion runs with pass/fail and actual values

Once that works end-to-end against Murphy's DataHub and Murphy's MSSQL, the rehearsal is real.

---

## 15. Open Questions (for Jianmin's review)

The v1 review settled domain, DB, table count, lineage granularity, and daily volume. Open spots that remain:

1. **MSSQL location — Murphy's existing instance vs. dedicated DB?** I've defaulted to Murphy (`192.168.0.16:1433`) since it's already there. Alternative: install MSSQL Express locally on the dev workstation so the ETL has no network dependency.
2. **MSSQL auth — SQL login vs. integrated Windows auth?** SQL login (`MSSQL_USER` + `MSSQL_PASSWORD`) is simpler from `pyodbc` and matches an enterprise service-account model. Integrated auth is possible but adds Kerberos / domain complexity not present in the lab.
3. **`CARD_INT.card_txn_decline` — physical table or view?** I kept it as a physical table to make table-level lineage interesting (2 downstream from `CARD_INT.card_txn`/`CARD_STG.card_auth`). A view would simplify the ETL but flatten the lineage demo.
4. **Reference data refresh cadence.** Pre-loaded once via `seed_reference.sql`. Alternative: refresh daily through the same orchestrator. I chose separation because reference data has different cadence than transactional data — typical enterprise pattern.
5. **500 rows/day default.** Confirmed. Sets `RECORD_COUNT_THRESHOLD` thin (100-1000). For higher-volume demos, generator's `--rows` flag is configurable.
6. **Top merchants — 10.** With 500 rows/day and ~100 merchants in the reference pool, top 10 gives a meaningful slice. Could go 5 or 20.
7. **Column-level lineage scope — full or sampled?** v2 design covers ~25 column edges across the downstream datasets. Could exhaustively cover every column or restrict to "interesting" derived columns. Defaulting to "every downstream column gets at least one upstream edge."
8. **`SOURCE_APP_NAME = CardCompass`.** Stamped on every DataHub payload's `customProperties["source.app"]`. Stable across runs — downstream consumers (DF360 if used) route on it.

---

## 16. The Application in One Sentence

> **A daily Python ETL that ingests credit-card authorization + settlement feeds into a 6-table MSSQL stage → integration → report pipeline, and publishes catalog, table-and-column lineage, and 43 assertion runs (30 firm-mandatory + 13 custom) per execution to DataHub via the local Auth Proxy — exercising the full push-only enterprise pattern end-to-end.**

---

*Companions:*
- *[`DataHub_Auth_Proxy_Pattern.md`](DataHub_Auth_Proxy_Pattern.md) — why the Auth Proxy exists*
- *[`Auth_Proxy_Design.md`](Auth_Proxy_Design.md) — the proxy this application talks to*
