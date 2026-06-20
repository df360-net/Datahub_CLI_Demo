# Sales Warehouse — Analytical Model & Medallion ETL Design

The lakehouse side of the pipeline: it consumes the feed in MinIO `landing/` and builds a queryable
**star schema** in Iceberg. This is the **T + L** of ETL (the feed extractor was the E). Companion
docs: `Sales_OLTP_Design.md` (source) and `Sales_Feed_Design.md` (feed). The engine is **Spark**;
the catalog is **Nessie**; the serving layer (next doc) is **Dremio**.

> **Scope.** This document designs the data model and the transformation logic — the **bronze →
> silver → gold** medallion, the dimensions and fact, surrogate keys, SCD-2, and how the watermark
> feed maps to idempotent upserts. Spark job code lives in `sales_oltp_app/etl/`; this doc is the
> design of record.

---

## 1. The medallion architecture — and your Teradata bridge

Three layers, each an Iceberg namespace, each a transformation with a clear contract:

```
landing/ (CSV)  ──▶  sales_bronze  ──▶  sales_silver  ──▶  sales_gold
  raw feed           raw, typed,        cleansed,           star schema:
  + manifest         appended as-is     conformed,          dims (SCD-2) + fact_sales
                     (immutable log)    deduped, current     (the serving model)
```

| Medallion | This warehouse | **Teradata bridge** | Shape |
|---|---|---|---|
| **Bronze** | raw landed rows → Iceberg, typed minimally, append-only with ingest metadata | **staging / landing** tables (truncate-load, raw) | 1:1 with source tables |
| **Silver** | cleansed, typed, deduped, conformed business entities (current state) | **core / 3NF integration** layer | normalized, keyed by business key |
| **Gold** | denormalized **star**: conformed dimensions (SCD-2) + `fact_sales` | **semantic / presentation** layer (your star schemas) | dims + fact, surrogate-keyed |

> The shape is identical to the EDW you've built for years — stage → core → semantic. What changes
> is the *physics underneath*: no enforced PK/FK (integrity becomes a DQ assertion), `MERGE INTO`
> instead of `UPSERT`/`IDENTITY`, partition transforms instead of PPIs, hash surrogate keys instead
> of sequences, and every write is an immutable Iceberg snapshot you can time-travel.

---

## 2. Namespaces & naming

| Concept | Form | Example |
|---|---|---|
| Bronze namespace | `sales_bronze` | `nessie.sales_bronze.orders` |
| Silver namespace | `sales_silver` | `nessie.sales_silver.orders` |
| Gold namespace | `sales_gold` | `nessie.sales_gold.fact_sales` |
| Dimension table | `dim_<entity>` | `dim_customer`, `dim_product`, `dim_store`, `dim_date` |
| Fact table | `fact_<process>` | `fact_sales` |
| Surrogate key | `<entity>_key` (BIGINT, hash) | `customer_key` |
| Business/natural key | `<entity>_id` (from source) | `customer_id` |
| SCD-2 control columns | `valid_from`, `valid_to`, `is_current`, `row_hash` | |

Tables are 100% Iceberg via Nessie (one catalog shared with Dremio). Spark jobs:
`etl/bronze_load.py`, `etl/silver_load.py`, `etl/gold_dims.py`, `etl/gold_fact.py`.

---

## 3. Bronze — the immutable raw log

**Contract:** land the feed *as received*, lose nothing, transform almost nothing. One bronze table
per source table (`orders`, `order_items`, `customers`, `products`, `product_categories`, `stores`).

- **Read** `s3://landing/sales/<run_date>/<table>_<run_date>.csv` (schema-on-read), **validate row
  counts against `_manifest.json`** (control-total check — fail fast if the feed is incomplete).
- **Type** columns explicitly (CSV is all strings): cast ids to BIGINT, money to `DECIMAL`, dates/ts
  to `DATE`/`TIMESTAMP`. Bad casts → quarantine, don't silently null.
- **Append** with ingest metadata columns: `_run_date`, `_source_file`, `_ingested_at`. Never update
  or dedupe here — bronze is the **append-only audit log** of what arrived.
- Partition bronze by `_run_date` (`days`) so each feed run is an isolated, prunable slice.

> **Teradata bridge.** Bronze = your staging tables, except *immutable and accumulating* instead of
> truncate-and-reload. The Iceberg snapshot history gives you "what did feed run N deliver" for free
> — replayable, auditable. Keep it; expire old snapshots on a long horizon.

---

## 4. Silver — cleansed, conformed, current

**Contract:** turn the raw append-log into clean, typed, **deduplicated current-state** business
entities — the integration layer the gold star is built from.

- **Deduplicate to latest:** a bronze table accumulates every version of a row ever fed. Silver keeps
  the **latest per business key** (window by `business_key` ordered by `updated_at` desc, take rank 1).
- **Conform & clean:** trim/standardize strings, normalize casing, enforce domains (valid `segment`,
  `status`, `channel`), resolve obvious DQ issues, derive helper columns.
- **Keep it normalized** (business-key'd, not yet denormalized): `silver.orders`, `silver.order_items`,
  `silver.customers`, `silver.products` (joined to category name here — the one denormalization that
  feeds `dim_product`), `silver.stores`.
- Idempotent: `MERGE INTO silver … ON business_key` (upsert), so re-running a feed is a no-op.

> **Teradata bridge.** Silver = the core/integration layer: one clean, current row per entity, keyed
> by the natural key, ready to feed presentation. The "dedupe to latest by updated_at" is the
> lakehouse stand-in for your incremental core-merge logic.

---

## 5. Gold — the star schema

The serving model. `fact_sales` at **order-line grain**, surrounded by four conformed dimensions.

```
              dim_date
                 │
 dim_customer ── fact_sales ── dim_product
                 │
              dim_store
```

### 5.1 Grain & fact_sales

**Grain: one row per order line** (`order_id`, `line_no`) — the same grain as `silver.order_items`.

| Column | Type | Role |
|---|---|---|
| `sales_key` | BIGINT | surrogate PK (hash of `order_id`,`line_no`) |
| `date_key` | INT | FK → `dim_date` (from `order_date`) |
| `customer_key` | BIGINT | FK → `dim_customer` (**point-in-time**, §7) |
| `product_key` | BIGINT | FK → `dim_product` (**point-in-time**) |
| `store_key` | BIGINT | FK → `dim_store` (**point-in-time**) |
| `order_id`, `line_no` | BIGINT/INT | **degenerate dimensions** (no dim table) |
| `status` | STRING | order status (degenerate; updated via CDC) |
| `quantity` | INT | additive measure |
| `unit_price` | DECIMAL(10,2) | measure (point-in-time price at sale) |
| `discount_pct` | DECIMAL(5,4) | measure |
| `extended_amount` | DECIMAL(12,2) | additive measure (= `line_amount` from source) |

- **Measures are additive** (`quantity`, `extended_amount`) — the analyst can `SUM` across any dim.
- **Degenerate dimensions** (`order_id`, `line_no`, `status`) ride on the fact with no dim table —
  exactly the Kimball pattern.
- Partition by `days(order_date)` (via `date_key`/a retained `order_date` column); sort by
  `customer_key` for join locality.

### 5.2 The four dimensions

| Dim | Source | Type | SCD-2 on | Notes |
|---|---|---|---|---|
| `dim_date` | **generated** | static | — | calendar; not from OLTP. `date_key` = `yyyymmdd` INT |
| `dim_customer` | `silver.customers` | **SCD-2** | `segment`, city/state | the headline slowly-changing dim |
| `dim_product` | `silver.products` (+category) | **SCD-2** | `unit_price`, `product_name`, `category` | denormalized: category folded in |
| `dim_store` | `silver.stores` | **SCD-2** | `region`, `channel`, `store_name` | versioned for as-of fidelity |

> **Design note — all entity dimensions are SCD-2 (settled).** `dim_customer`, `dim_product`, and
> `dim_store` are uniformly Type-2. Rationale: **100% point-in-time ("as-of") reporting requires
> every dimension to be versioned** — a single Type-1 dim overwrites its attributes, which silently
> breaks any attempt to reconstruct exact historical state for that dimension. Uniform Type-2 also
> keeps the load logic identical across every dim. (`dim_date` is exempt — a calendar's attributes
> never change, so versioning is N/A.)

`dim_date` is generated once (a calendar from, say, 2020-01-01 forward) with `date_key`, `date`,
`year`, `quarter`, `month`, `day`, `day_of_week`, `is_weekend`, etc. — the standard date dimension.

---

## 6. Surrogate keys — hash, not sequence

Lakehouse has no `IDENTITY`/sequence (no central coordinator across distributed writers). We use
**deterministic hash surrogate keys**:

- **Non-versioned key** (fact, dim_date): `xxhash64(business_key…)`.
  `sales_key = xxhash64(order_id, line_no)`;
  `date_key = cast(date_format(order_date,'yyyyMMdd') as int)`.
- **SCD-2 key** (customer, product, store): hash must change per *version*, so include the version
  discriminator: `customer_key = xxhash64(customer_id, valid_from)`,
  `store_key = xxhash64(store_id, valid_from)`.

> **Teradata bridge.** You'd reach for an `IDENTITY` column; the lakehouse equivalent is a hash,
> because it's **deterministic and needs no coordination** — every Spark task computes the same key
> independently, loads are idempotent (re-running yields identical keys), and there's no sequence to
> contend on. The price: keys are wide-ish (64-bit) and opaque — fine for a columnar engine.

> **Design note — collisions.** `xxhash64` is 64-bit; collision risk at our scale is negligible. If
> you want zero risk, use the natural composite key directly and skip surrogates — but surrogates buy
> you SCD-2 versioning and a single narrow join column, so we keep them.

---

## 7. SCD-2 mechanics & point-in-time fact resolution

The two hardest, most important mechanics — and where lakehouse differs most from Teradata.

### 7.1 SCD-2 load (the two-step MERGE)

Each Type-2 dim row carries `valid_from`, `valid_to` (`9999-12-31` while open), `is_current`, and a
`row_hash` over the **tracked** attributes. To apply a batch of changed entities from silver:

1. **Detect change** — join incoming (silver current) to the dim's `is_current` rows on business key;
   a row is *changed* if `row_hash` differs, *new* if no current row exists.
2. **MERGE step 1 — close** the changed current rows: `is_current=false`, `valid_to=<batch_ts>`.
3. **MERGE/append step 2 — open** a new current version for every *new* and *changed* key:
   `valid_from=<batch_ts>`, `valid_to='9999-12-31'`, `is_current=true`, fresh `customer_key`.

> **Why two steps:** a single `MERGE INTO` can't both update the old version *and* insert a new
> version for the same key (one source row → at most one matched action). Close-then-open is the
> standard Iceberg/Spark SCD-2 pattern. (Teradata you'd do the same logically; here it's two explicit
> MERGEs, and each is an atomic Iceberg snapshot.)

### 7.2 Point-in-time FK resolution in the fact

When loading `fact_sales`, resolve each line's `customer_key`/`product_key`/`store_key` to the
dimension version that was **current at the order's business date** — not whatever is current now
(the same `valid_from`/`valid_to` join applies to all three Type-2 dims):

```sql
JOIN dim_customer d
  ON  f.customer_id = d.customer_id
  AND f.order_date >= d.valid_from
  AND f.order_date <  d.valid_to
```

So a March order points to the customer's March segment, even if they upgraded to ENTERPRISE in June.
This is the entire payoff of Type-2 — historical facts keep their historical context. Existing fact
rows are **never re-pointed** when a dim changes; only new facts pick up the new version.

> **Onboarding nuance:** the backfill loads each dim as a single version with `valid_from` = an early
> sentinel (e.g. `1900-01-01`), so all 90 days of historical facts resolve cleanly to v1. Drift from
> daily runs forward then creates v2+, and new orders resolve to the then-current version.

---

## 8. Fact load — upsert, status changes, COW vs MOR

- **Idempotent upsert** keyed by the grain: `MERGE INTO fact_sales … ON (order_id, line_no)`. New
  lines insert; **re-fed changed orders** (status transitions) update the existing fact row's
  `status` (and re-resolve nothing else). Safe to re-run.
- **Status propagation:** the feed re-emits a changed `orders` row but not its unchanged lines (per
  `Sales_Feed_Design.md`). The fact load joins incoming changed orders to existing fact rows by
  `order_id` and updates `status` across that order's lines.
- **COW vs MOR:** `fact_sales` is read-heavy and updated in modest batches (status flips) → **start
  COW** (copy-on-write: simplest, fastest reads). Switch to MOR only if status-update write
  amplification becomes a problem. Schedule periodic `rewrite_data_files` + `expire_snapshots`
  (the lakehouse REORG).

---

## 9. Idempotency, incrementality, partitioning

- **Every layer is a MERGE keyed by its business/grain key** → the whole pipeline is re-runnable; a
  replayed feed produces no duplicates and no drift.
- **Incremental by the feed:** bronze ingests only the run's files; silver/gold process only the keys
  that changed (driven off bronze's `_run_date` slice). First run = full history (onboarding);
  steady state = a day's deltas.
- **Partitioning:** `fact_sales` by `days(order_date)`; bronze by `days(_run_date)`; dims unpartitioned
  (small). Sort `fact_sales` by `customer_key`. Revisit `months(order_date)` if daily partitions get
  too small.

---

## 10. Data quality (DQ) gates

Checks run as part of the ETL (and become **DataHub assertions** in the Wave-5 capstone):

- **Control total:** bronze row counts == `_manifest.json` counts (feed completeness).
- **Reconciliation:** `SUM(extended_amount)` per order == `orders.order_total` (the deliberate hook
  from the OLTP design — catches the kind of rounding bug we already hit once).
- **Referential:** every `fact_sales` FK resolves to a dim row (no orphan keys).
- **Uniqueness/grain:** `fact_sales` unique on (`order_id`,`line_no`); each Type-2 dim has exactly one
  `is_current` row per business key.
- **Not-null:** business keys never null.

> **Teradata bridge.** These are the integrity constraints the source enforced for you (PK/FK/CHECK)
> — in the lakehouse they're **assertions you run**, not constraints the engine guarantees. Same
> safety, different placement: validate, don't assume.

---

## 11. Orchestration (preview)

The daily Airflow DAG (step 7) sequences the Spark jobs, each `docker exec spark-iceberg spark-submit`:

```
simulate-day (generator) → extract-feed → bronze_load → silver_load → gold_dims → gold_fact → DQ checks → (Dremio refresh)
```

`gold_dims` before `gold_fact` (the fact resolves dim keys, so dims must be current first). Each job
is idempotent, so a failed run re-runs cleanly from the top.

---

## 12. Teradata cheat-sheet

| Teradata concept | Sales warehouse (Iceberg/Spark) equivalent |
|---|---|
| Staging tables (truncate-load) | **bronze** (append-only, immutable, snapshotted) |
| Core / 3NF integration layer | **silver** (cleansed, deduped, business-key'd) |
| Semantic / star schema | **gold** (`dim_*` + `fact_sales`) |
| `IDENTITY` surrogate key | **hash surrogate** (`xxhash64`, deterministic, no coordination) |
| `UPSERT` / `MERGE` | `MERGE INTO` (atomic Iceberg snapshot) |
| Temporal / SCD-2 via valid-time | `valid_from`/`valid_to`/`is_current` + **two-step MERGE** |
| Point-in-time dimension lookup | FK resolution `order_date BETWEEN valid_from AND valid_to` |
| Degenerate dimension | same concept — `order_id`/`line_no`/`status` on the fact |
| PPI partitioning | `days(order_date)` partition transform |
| `COLLECT STATISTICS` | Iceberg/Parquet stats, written with the data, never stale |
| PK/FK/CHECK constraints | **DQ assertions** run in-ETL (not engine-enforced) |
| REORG / PACK | `rewrite_data_files` + `expire_snapshots` |

---

*Spark job code lands in `sales_oltp_app/etl/`. This document is the warehouse design of record; the
serving layer (Dremio) and BI (Superset) have their own docs. Build order: bronze → silver →
gold_dims → gold_fact, each verified against Murphy/Zeenie before the next.*
