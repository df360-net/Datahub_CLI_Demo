# Sales Feed — Extraction Design

Process #2 of the Sales OLTP app: the **feed extractor**. It is the only boundary-crossing step on
the source side — it reads the OLTP (read-only) and writes the daily feed to the lakehouse landing
area. This is the **"E"** of ETL; the **T** and **L** happen later, in Spark, on the lakehouse side.
The companion documents are `Sales_OLTP_Design.md` (the source) and `Sales_Warehouse_Design.md`
(the target, forthcoming).

> **Scope.** The extractor moves bytes; it does not transform them. It emits the OLTP's current rows
> as CSV plus a manifest, into `s3://landing/sales/<run_date>/`. It knows the source schema and the
> landing layout — nothing about Iceberg, dimensions, or the star schema.

---

## 1. Role & guarantees

- **Read-only against the source.** Connects as `sales_extract` (SELECT only). It can never mutate
  `sales_oltp`. Its own state (the watermark) lives outside the source — see §3.
- **Incremental, lossless, idempotent.** One uniform rule (the watermark, §2) captures new + changed
  rows with no gaps and no dependence on a separate initial-load job. Re-running a date is safe.
- **Completion is explicit.** A run is "done" only when `_manifest.json` is written last; the
  downstream treats the manifest as the signal to process the run.

---

## 2. The watermark model (the core)

Every table is extracted by a **per-table high-watermark on `updated_at`**. Pseudo-algorithm for one
run:

```
run_hwm = SELECT NOW()                      -- one upper bound for the whole run
for each table T in [product_categories, customers, products, stores, orders, order_items]:
    low = state[T]                          -- last run's watermark; epoch on the very first run
    rows = SELECT * FROM T
           WHERE updated_at >= low AND updated_at <= run_hwm
    write rows -> s3://landing/sales/<run_date>/<T>_<run_date>.csv
write _manifest.json                        -- the completion marker, written LAST
for each table T:  state[T] = run_hwm       -- advance only after a successful write
```

Why each piece:

- **`low` inclusive (`>=`).** `updated_at` is second-granularity; an inclusive lower bound re-emits the
  boundary second's rows on the next run. That tiny overlap is harmless because the warehouse `MERGE`s
  by primary key (idempotent). Inclusive-low + idempotent-merge = **zero data loss**, which beats a
  strict `>` that can drop same-second rows.
- **`run_hwm` upper bound (`<= NOW()` captured once).** Rows written *during* the extraction get a
  timestamp after `run_hwm`, so they're deferred to the next run rather than half-captured across
  tables. Stable, consistent cut.
- **First run, `low = epoch`** → `updated_at >= '1970-01-01'` matches everything → the full history is
  the onboarding load. No separate backfill job. From run 2 on, only new/changed rows flow.
- **Advance after success.** If a run fails mid-way, the watermark isn't advanced, so the next run
  re-extracts the same window — safe because of idempotency.

This is CDC-lite: a disciplined audit column, no binlog reader. It is complete here because the OLTP
**never hard-deletes** (cancellations/returns are `status` updates that bump `updated_at`).

---

## 3. Where watermark state lives — DECIDED: MinIO

The extractor needs to persist its watermark between runs. Two candidates were weighed:

| Option | Pros | Cons |
|---|---|---|
| **MinIO JSON object** (chosen) | keeps `sales_extract` **strictly read-only** on the source; state co-located with the output; portable; no new DB grant | watermark + source read aren't one transaction (fine — we advance only after a successful write) |
| MySQL control table `_feed_watermark` | transactional with the read; inspectable in Workbench | requires a **write grant**, breaking the read-only principle for the extractor |

> **Reversal of my earlier lean.** I initially leaned to a MySQL control table, but the **read-only
> principle wins**: granting the extractor write access to the source just to store its own bookmark
> contradicts least privilege. State belongs with the *output*, not the *source*. So the watermark is
> a JSON object in MinIO:

```
s3://landing/sales/_state/feed_watermark.json
{ "product_categories": "2026-06-19T23:00:00",
  "customers":          "2026-06-19T23:00:00",
  ...,
  "order_items":        "2026-06-19T23:00:00" }
```

The extractor reads/writes this object (it already has S3 write to the landing bucket); it needs
**zero** write access to MySQL.

---

## 4. Output layout & format

```
s3://landing/sales/2026-06-19/
    product_categories_2026-06-19.csv
    customers_2026-06-19.csv
    products_2026-06-19.csv
    stores_2026-06-19.csv
    orders_2026-06-19.csv
    order_items_2026-06-19.csv
    _manifest.json
```

- The `<run_date>` prefix is the **extraction date** (run 1 / onboarding holds all history; later runs
  hold a day's changes). Business dates live *inside* the data (`orders.order_date`).
- **CSV format:** UTF-8, header row, comma-delimited, `\r\n`-free (`\n`), `NULL` → empty field,
  dates/timestamps ISO-8601, `DECIMAL` as plain decimal strings (no thousands separators). Minimal
  quoting (quote only fields containing delimiter/quote/newline).
- **`_manifest.json`** — written last, the run's completion + control-total record:

```json
{
  "source": "sales_oltp",
  "run_date": "2026-06-19",
  "extracted_at": "2026-06-19T23:05:12-04:00",
  "run_hwm": "2026-06-19T23:00:00",
  "tables": [
    {"table": "orders",      "file": "orders_2026-06-19.csv",      "low_watermark": "1970-01-01T00:00:00", "rows": 11200},
    {"table": "order_items", "file": "order_items_2026-06-19.csv", "low_watermark": "1970-01-01T00:00:00", "rows": 33505}
  ]
}
```

The row counts are the control totals the lakehouse verifies after ingest.

---

## 5. Connection & configuration

All config-driven (12-factor), so the same code runs from Lenovo in dev and the airflow container in
prod — only env differs. Additions to `sales_oltp_app/.env`:

```
MINIO_ENDPOINT=http://192.168.0.21:9000      # prod (airflow container): http://minio:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password
LANDING_BUCKET=landing
```

- **Source:** `sales_db.get_engine("extract")` → `sales_extract` (read-only) over the LAN
  (`192.168.0.21:3306`) in dev, `host.docker.internal:3306` in prod.
- **Sink:** MinIO via **boto3** S3 client (path-style, non-TLS LAN). `boto3` is added to
  `requirements.txt`. Endpoint/keys/bucket from env.
- **The `landing` bucket must exist** in MinIO (created alongside `warehouse`). One-time setup.

---

## 6. Idempotency, atomicity, re-runs

- **In-memory CSV → `put_object`.** Each table's CSV is built in memory (feeds are MB-scale) and
  uploaded in one `put_object`. A partially written run leaves no manifest → not consumed.
- **Manifest-last** is the atomic completion marker. Watermark advances only after the manifest writes.
- **Re-running a date** re-extracts from the (un-advanced) watermark and overwrites that date's
  objects. Downstream `MERGE` makes reprocessing a no-op. Safe by construction.

---

## 7. Runtime topology

Same model as `Sales_OLTP_Design.md` §9. In dev the extractor runs on Lenovo (reads MySQL + writes
MinIO over the LAN). In prod it runs as an Airflow `PythonOperator` in the airflow container, reaching
MySQL via `host.docker.internal` and MinIO via `minio:9000`. The daily DAG sequences:
`simulate-day (generator) → extract-feed → (Spark ETL)`.

---

## 8. Boundary — what the extractor does NOT do

- No transformation, typing, or cleansing (Spark's job in bronze→silver).
- No dimensional logic, no surrogate keys, no SCD (warehouse's job).
- No deletes detection beyond soft-delete status updates (by design — see §2).
- No direct talk to Iceberg/Nessie/Dremio. It writes CSV to a bucket; that is the entire contract.

---

## 9. Open items

- **Watermark reset / replay** — a `--full` flag that ignores stored state (resets `low` to epoch) for
  a clean re-onboarding; useful in dev.
- **Compression** — gzip the CSVs (`.csv.gz`) once volumes grow; trivial to add to `put_object`.
- **Manifest validation hook** — the lakehouse bronze job should assert file row counts vs the
  manifest before processing (control-total check). Specified on the warehouse side.
- **Run-date prefix = extraction calendar day** (`NOW().date()`), assuming one run per real day (the
  daily DAG cadence). A second same-day run overwrites that day's objects. Fix when wiring Airflow:
  accept a logical `--run-date` (Airflow's `{{ ds }}`) so the prefix is the orchestrated logical date,
  decoupled from wall-clock; the `updated_at` watermark still drives incrementality. Until then, dev
  re-runs on the same real day overwrite — use `--full` deliberately, and don't re-run on a day whose
  full load hasn't been ingested yet.

---

*Implementation: `sales_oltp_app/tools/extract_feed.py`, reusing `tools/sales_db.py` for the source
connection and a small boto3 helper for MinIO. This doc is the feed design of record.*
