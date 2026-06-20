# Sales OLTP — Application & Data Model Design

The **source system** for the lakehouse pipeline. This document designs the OLTP application end to
end: its purpose, its normalized data model (MySQL 8.0), the data lifecycle (seed + daily activity),
and — critically — the **feed contract** it emits to the lakehouse drop folder. It is authoritative
for *what the source is and what it produces*; the Spark ETL and the analytical star schema are
designed in their own documents (`Sales_Warehouse_Design.md`, forthcoming).

> **Scope boundary.** This app's job ends at the drop folder. It is a self-contained order-entry
> database that knows **nothing** about Iceberg, Spark, Dremio, or DataHub — exactly as a real
> production OLTP system is unaware of the warehouse that consumes it. The decoupling is deliberate:
> the OLTP writes files to a folder; the lakehouse reads them. That folder is the only contract.

---

## 1. Purpose & role in the pipeline

```
  ┌─────────────────────┐     daily      ┌──────────────┐    Spark ETL    ┌──────────────────┐
  │   Sales OLTP         │    feeds       │MinIO landing/│   bronze→silver │  Iceberg star    │
  │   (MySQL, 3NF)       │ ─────────────▶ │  (CSV files) │ ──────────────▶ │  (dims + fact)   │
  │   order-entry system │  extract       │  + manifest  │   →gold         │  via Nessie      │
  └─────────────────────┘                └──────────────┘                 └──────────────────┘
        THIS DOCUMENT                       the contract                    Sales_Warehouse_Design.md
```

The Sales OLTP simulates a retail order-entry system: customers place orders at stores/channels,
each order has line items referencing a product catalog. It is **write-optimized and normalized**
(3NF) — the opposite of the read-optimized star schema it will feed. The transformation between the
two *is the lesson*: the Spark ETL denormalizes and conforms this model into dimensions and a fact.

**What this app deliberately does** (to make the downstream interesting):
- Grows daily — new orders, occasionally new customers/products.
- **Mutates** — order `status` advances after creation (PLACED → SHIPPED → DELIVERED), some orders
  cancel/return. This drives **incremental re-extraction** and **fact updates** downstream.
- **Drifts** — product prices change, customers change segment. This is the raw material for
  **SCD-2** (slowly changing dimensions) in the warehouse.

**What it deliberately is not**: no analytics, no aggregates, no history tables. History is the
warehouse's job. The OLTP holds only *current state* plus audit timestamps.

---

## 2. Design principles

1. **Normalized (3NF), surrogate-keyed.** Every entity has an auto-increment surrogate PK; natural
   keys (email, SKU) carry `UNIQUE` constraints. Reference data is factored out (`product_categories`).
   Redundancy is avoided except one controlled denormalization (§5, `orders.order_total`), flagged
   explicitly.
2. **Referential integrity is enforced here.** InnoDB foreign keys guarantee a clean source. (Sharp
   contrast for the lakehouse: Iceberg has **no** enforced PK/FK — integrity becomes a DQ *assertion*,
   not a constraint. That contrast is a teaching point, not an accident.)
3. **Every table carries `created_at` + `updated_at` audit columns.** These are not decoration — the
   `updated_at` watermark is the mechanism for **incremental, change-aware extraction** (§7). This is
   CDC-lite: no binlog reader needed, just disciplined audit columns.
4. **Money is `DECIMAL`, never `FLOAT`.** Binary floats can't represent currency exactly; all amounts
   and prices are fixed-point `DECIMAL`.
5. **Point-in-time price capture.** An order line stores the `unit_price` *at the moment of sale*,
   independent of the product's current list price. Historical orders must never re-price when the
   catalog changes — a fundamental OLTP correctness rule.
6. **Least privilege.** DDL is the admin's job (root). The application uses a DML-only user; the feed
   extractor uses a read-only user (§8). Mirrors the `sa` / `DCFDBUSR` discipline from CardCompass.

> **RDBMS bridge.** None of this is new to you — it's textbook OLTP modeling. The point of writing it
> down is the *contrast* with the warehouse: normalized vs star, enforced FK vs DQ assertion,
> current-state vs full-history, surrogate-in-source vs surrogate-regenerated-in-dim.

---

## 3. Naming conventions (settled)

| Concept | Convention | Example |
|---|---|---|
| Database | lowercase snake | `sales_oltp` |
| Tables | lowercase snake, **plural** | `customers`, `order_items` |
| Columns | lowercase snake | `customer_id`, `order_ts` |
| Primary key | `<entity_singular>_id` | `customer_id` |
| Foreign key | same name as referenced PK | `orders.customer_id` |
| Unique constraint | `uq_<table>_<col>` | `uq_customers_email` |
| Index | `ix_<table>_<col>` | `ix_orders_order_date` |
| FK constraint | `fk_<table>_<ref>` | `fk_orders_customer` |
| Check constraint | `chk_<table>_<rule>` | `chk_order_items_qty` |
| App brand / `source.app` | `Sales` | (used downstream in DataHub) |
| Lakehouse namespace | `sales` | `nessie.sales.fact_sales` |

MySQL on Windows stores table names lower-case (`lower_case_table_names=1`) and treats them
case-insensitively — lowercase snake is the idiomatic, friction-free choice. **Engine:** InnoDB
(transactional, FK-capable). **Charset:** `utf8mb4` / `utf8mb4_0900_ai_ci`.

---

## 4. Entity–relationship overview

```
 product_categories
        │ 1
        │
        ▼ N
     products ─────────────┐
        │ 1                │ N
        │                  │
        ▼ N                │
   order_items  N────────▶ │ (product_id)
        │ N                
        │ (order_id)       
        ▼ 1                
      orders  N────────▶ 1  customers
        │ N
        │ (store_id)
        ▼ 1
      stores
```

- A **category** has many **products**; a **product** appears on many **order lines**.
- An **order** belongs to one **customer** and one **store**, and has many **order_items** (lines).
- An **order line** references one **product**.
- Grain of `order_items` = one row per (order, line) = **the future grain of `fact_sales`**.

**Reference (slow-changing) tables:** `product_categories`, `products`, `stores`, `customers`.
**Transactional (fast-growing) tables:** `orders`, `order_items`.

---

## 5. Table specifications (DDL)

The DDL below is the authoritative schema. It will be split into runnable files under
`sales_oltp_app/db/` (`00_create_db.sql`, `01_schema.sql`, `02_users.sql`, `03_seed_reference.sql`).

```sql
CREATE DATABASE IF NOT EXISTS sales_oltp
  CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE sales_oltp;
```

### 5.1 product_categories  (reference)

```sql
CREATE TABLE product_categories (
  category_id    INT UNSIGNED    NOT NULL AUTO_INCREMENT,
  category_name  VARCHAR(80)     NOT NULL,
  created_at     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (category_id),
  UNIQUE KEY uq_product_categories_name (category_name)
) ENGINE=InnoDB;
```

### 5.2 customers  (reference, SCD-2 source)

```sql
CREATE TABLE customers (
  customer_id  BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  first_name   VARCHAR(60)      NOT NULL,
  last_name    VARCHAR(60)      NOT NULL,
  email        VARCHAR(255)     NOT NULL,
  segment      VARCHAR(20)      NOT NULL DEFAULT 'CONSUMER',
  city         VARCHAR(80)              ,
  state        VARCHAR(40)              ,
  country      VARCHAR(40)      NOT NULL DEFAULT 'USA',
  signup_date  DATE             NOT NULL,
  created_at   TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (customer_id),
  UNIQUE KEY uq_customers_email (email),
  KEY ix_customers_updated_at (updated_at),
  CONSTRAINT chk_customers_segment CHECK (segment IN ('CONSUMER','SMB','ENTERPRISE'))
) ENGINE=InnoDB;
```

> **Design note — `segment` as `VARCHAR + CHECK`, not `ENUM`.** ENUM is MySQL-specific and exports as
> an ordinal in some tools, which fouls downstream ETL. A constrained `VARCHAR` flows to the warehouse
> as a clean string and stays portable. `segment` is the demo's **SCD-2 attribute**: when a customer
> moves CONSUMER → ENTERPRISE, the warehouse closes the old dim row and opens a new one.

### 5.3 products  (reference, SCD-2 source)

```sql
CREATE TABLE products (
  product_id    BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  sku           VARCHAR(40)      NOT NULL,
  product_name  VARCHAR(150)     NOT NULL,
  category_id   INT UNSIGNED     NOT NULL,
  unit_price    DECIMAL(10,2)    NOT NULL,          -- current list price (changes over time)
  active_flag   TINYINT(1)       NOT NULL DEFAULT 1,
  created_at    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (product_id),
  UNIQUE KEY uq_products_sku (sku),
  KEY ix_products_category (category_id),
  KEY ix_products_updated_at (updated_at),
  CONSTRAINT fk_products_category
    FOREIGN KEY (category_id) REFERENCES product_categories(category_id),
  CONSTRAINT chk_products_price CHECK (unit_price >= 0)
) ENGINE=InnoDB;
```

### 5.4 stores  (reference)

```sql
CREATE TABLE stores (
  store_id    INT UNSIGNED   NOT NULL AUTO_INCREMENT,
  store_name  VARCHAR(100)   NOT NULL,
  channel     VARCHAR(20)    NOT NULL DEFAULT 'RETAIL',   -- RETAIL / ONLINE / PARTNER
  region      VARCHAR(40)    NOT NULL,
  city        VARCHAR(80)            ,
  state       VARCHAR(40)            ,
  country     VARCHAR(40)    NOT NULL DEFAULT 'USA',
  created_at  TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (store_id),
  UNIQUE KEY uq_stores_name (store_name),
  CONSTRAINT chk_stores_channel CHECK (channel IN ('RETAIL','ONLINE','PARTNER'))
) ENGINE=InnoDB;
```

### 5.5 orders  (transactional, mutable)

```sql
CREATE TABLE orders (
  order_id     BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  customer_id  BIGINT UNSIGNED  NOT NULL,
  store_id     INT UNSIGNED     NOT NULL,
  order_ts     DATETIME         NOT NULL,                 -- exact placement timestamp
  order_date   DATE             NOT NULL,                 -- business date = DATE(order_ts); feed partition key
  status       VARCHAR(20)      NOT NULL DEFAULT 'PLACED',
  order_total  DECIMAL(12,2)    NOT NULL DEFAULT 0.00,    -- controlled denormalization (see note)
  created_at   TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (order_id),
  KEY ix_orders_customer (customer_id),
  KEY ix_orders_store (store_id),
  KEY ix_orders_order_date (order_date),
  KEY ix_orders_updated_at (updated_at),
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  CONSTRAINT fk_orders_store    FOREIGN KEY (store_id)    REFERENCES stores(store_id),
  CONSTRAINT chk_orders_status
    CHECK (status IN ('PLACED','PAID','SHIPPED','DELIVERED','CANCELLED','RETURNED'))
) ENGINE=InnoDB;
```

> **Design note — `order_total` is a controlled denormalization.** Strict 3NF would derive it by
> summing `order_items.line_amount`. We store it on the header (a common OLTP read-performance pattern:
> show an order total without scanning lines) and the application keeps it consistent. The upside for
> the lab: it creates a real **reconciliation DQ check** downstream — does `SUM(line_amount)` per order
> equal `order_total`? That becomes a `RECORD_COUNT`/integrity-style assertion in the warehouse.
>
> **Design note — `order_date` alongside `order_ts`.** Redundant by derivation, kept because it is the
> **feed partition key** and the natural Iceberg partition (`days(order_date)`). Indexed for date-range
> extraction.

### 5.6 order_items  (transactional, the future fact grain)

```sql
CREATE TABLE order_items (
  order_id      BIGINT UNSIGNED  NOT NULL,
  line_no       SMALLINT UNSIGNED NOT NULL,
  product_id    BIGINT UNSIGNED  NOT NULL,
  quantity      INT UNSIGNED     NOT NULL,
  unit_price    DECIMAL(10,2)    NOT NULL,                -- price AT TIME OF SALE (point-in-time)
  discount_pct  DECIMAL(5,4)     NOT NULL DEFAULT 0.0000, -- 0.0000 .. 0.9999
  line_amount   DECIMAL(12,2) AS (ROUND(quantity * unit_price * (1 - discount_pct), 2)) STORED,
  created_at    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (order_id, line_no),
  KEY ix_order_items_product (product_id),
  KEY ix_order_items_updated_at (updated_at),
  CONSTRAINT fk_order_items_order
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
  CONSTRAINT fk_order_items_product
    FOREIGN KEY (product_id) REFERENCES products(product_id),
  CONSTRAINT chk_order_items_qty  CHECK (quantity > 0),
  CONSTRAINT chk_order_items_disc CHECK (discount_pct >= 0 AND discount_pct < 1)
) ENGINE=InnoDB;
```

> **Design note — `line_amount` is a `STORED` generated column.** MySQL computes
> `quantity × unit_price × (1 − discount_pct)` on write and persists it. The measure is defined once,
> in the schema, and can never drift from its inputs — a small lesson in pushing derivation down to
> where the data lives. This column maps straight to the additive fact measure `extended_amount`.
>
> **Soft-delete by convention:** orders are not hard-deleted; they move to `CANCELLED`/`RETURNED`. The
> `ON DELETE CASCADE` is a safety net, not a workflow.

---

## 6. Data lifecycle — seed then daily activity

Generation is driven by a Python tool (`sales_oltp_app/tools/generate_daily_activity.py`; mechanics
in the feed doc). Conceptually:

**Seed (day 0 — one-time):**
- Reference data: ~6 categories, ~200 products across them, ~10 stores (mixed channels), ~2,000
  customers.
- A **backfill of history**: ~90 days of prior orders so the warehouse has a real initial load to
  build dimensions and a fact from, not an empty start.

**Daily run (business date `D`):**
1. **New orders** — generate the day's orders + lines (volume parameterized, e.g. 200–800/day), with
   realistic spread across customers, stores, products; `unit_price` snapshotted from the product's
   then-current price; small random discounts.
2. **Dimension drift** (low rate, to feed SCD-2):
   - occasionally insert a new customer or product;
   - bump a few product prices;
   - change a few customers' `segment` or address.
3. **Status transitions** — advance a sample of *prior* orders (PLACED→PAID→SHIPPED→DELIVERED),
   cancel/return a small fraction. Each `UPDATE` bumps `updated_at`, so the incremental feed re-emits
   the changed (old) rows.

This produces exactly the three downstream challenges the warehouse must solve: **append** (new
facts), **update** (changed facts), and **drift** (changed dimensions).

---

## 7. The feed contract — what the OLTP emits

The landing bucket is the boundary. Per run (one daily batch), the extractor produces a dated prefix
in the MinIO `landing/` bucket (see §9 for why object storage rather than a host folder):

```
s3://landing/sales/2026-06-19/
    product_categories_2026-06-19.csv     full snapshot
    customers_2026-06-19.csv              full snapshot
    products_2026-06-19.csv               full snapshot
    stores_2026-06-19.csv                 full snapshot
    orders_2026-06-19.csv                 incremental (new + changed)
    order_items_2026-06-19.csv            incremental (new + changed)
    _manifest.json                        control totals + watermark
```

**Extraction strategy — one uniform rule.** Every table is extracted **incrementally by a
high-watermark on `updated_at`**: each run emits rows where `updated_at > last_watermark`, then
advances the watermark to the max `updated_at` it saw. This single rule **subsumes the initial load** —
on the **first run the watermark is epoch (0)**, so every row is emitted (the full history = the
onboarding load); from the **second run on**, only new and changed rows flow. One code path, no
separate backfill job.

| Table | Watermark column | First run (watermark = 0) | Steady state |
|---|---|---|---|
| `product_categories`, `stores` | `updated_at` | all rows | changed rows (rare) |
| `customers`, `products` | `updated_at` | all rows | new + changed (SCD-2 source) |
| `orders` | `updated_at` | all rows | new orders + status changes |
| `order_items` | `updated_at` | all rows | new lines |

Correctness notes:
- **Business time rides in the data, not the predicate.** `orders.order_date` carries the true
  business date into every row, so the warehouse fact is dated correctly even though run 1 extracts 90
  days of history in one batch (all stamped with the onboarding instant's `updated_at`). Extract-time
  and business-time stay independent.
- **Soft-deletes keep CDC complete.** No hard deletes (cancellations/returns are `status` updates that
  bump `updated_at`), so a `updated_at`-watermark feed misses nothing — the gap that normally forces
  full snapshots doesn't exist here.
- **Status propagation.** When an order's status changes, its `orders` row re-flows but its
  `order_items` rows don't (lines didn't change). The warehouse applies the new status to that order's
  fact lines from the `orders` feed (warehouse-side; see the warehouse doc).

**Format:** CSV, UTF-8, header row, comma-delimited, `\n` line endings, ISO dates/timestamps,
`NULL` rendered as empty field. (CSV because real enterprise feeds are usually delimited files, and
the bronze layer should practice schema-on-read. Parquet is a possible later optimization.)

**`_manifest.json`** — the completeness/control contract, so the lakehouse can verify it received a
whole, correct feed before processing (a real-world control-total practice):

```json
{
  "business_date": "2026-06-19",
  "extracted_at": "2026-06-19T23:05:12-04:00",
  "source": "sales_oltp",
  "watermark": "2026-06-19T23:00:00-04:00",
  "files": [
    {"table": "orders",      "file": "orders_2026-06-19.csv",      "rows": 412, "strategy": "incremental"},
    {"table": "order_items", "file": "order_items_2026-06-19.csv", "rows": 987, "strategy": "incremental"},
    {"table": "customers",   "file": "customers_2026-06-19.csv",   "rows": 2014, "strategy": "snapshot"}
  ]
}
```

**Watermark state** is tracked **per table** (each table's last max `updated_at`), persisted between
runs so the next run resumes exactly where it stopped — no gaps, no double-processing. The first run
starts from epoch, making it a full extract; every run after is incremental. CDC-lite on the audit
column, no binlog required. (Where the watermark state lives — a small control table in MySQL vs a
JSON object in MinIO — is decided in `Sales_Feed_Design.md`.)

> The detailed extractor (connection, driver, file writing, manifest generation, watermark store) is
> specified in `Sales_Feed_Design.md`. This document fixes only the **contract**: file set, naming,
> per-table strategy, format, and manifest shape.

---

## 8. Security & access

Mirrors the DDL-vs-DML separation used in CardCompass:

```sql
-- DDL/admin: root only (schema changes, user management) — not used by the app at runtime.

-- Application user: database-wide DML, no DDL.
CREATE USER 'sales_app'@'%' IDENTIFIED BY '<set-in-.env>';
GRANT SELECT, INSERT, UPDATE, DELETE ON sales_oltp.* TO 'sales_app'@'%';

-- Feed extractor: read-only (the feed must never mutate the source).
CREATE USER 'sales_extract'@'%' IDENTIFIED BY '<set-in-.env>';
GRANT SELECT ON sales_oltp.* TO 'sales_extract'@'%';
```

- The generator/app connects as `sales_app` (DML). The daily feed connects as `sales_extract`
  (read-only). Spark, when it later reads MySQL directly (if ever), would also use `sales_extract`.
- **No secrets in code or git.** Passwords live in a gitignored `.env`. The `<...>` placeholders above
  are filled at run time, never committed.
- MySQL is bound to the LAN (`0.0.0.0:3306`, firewall open) so Workbench@Lenovo and the Spark
  container (`host.docker.internal:3306` or `192.168.0.21:3306`) can reach it. Auth plugin is
  `caching_sha2_password`; JDBC clients add `allowPublicKeyRetrieval=true` over the non-TLS LAN.

---

## 9. Deployment & runtime topology

Development happens on Lenovo (where the codebase lives and is edited); **production execution happens
on Zeenie, orchestrated by Airflow.** The *same* code runs in both places because every job is
**config-driven** — connection coordinates and the landing bucket come from environment / `.env`,
never hardcoded.

**Airflow orchestrates heterogeneous runtimes.** It does not execute everything in-process; it
triggers each job in the container that owns that runtime:

| Job | Runtime (where it executes) | How Airflow drives it |
|---|---|---|
| generate daily activity (Python → MySQL) | airflow container | `PythonOperator` (or `docker exec`); needs PyMySQL+SQLAlchemy installed there; reaches MySQL via `host.docker.internal:3306` |
| feed extract (Python → MySQL → CSV → MinIO) | airflow container | same |
| **Spark ETL** (bronze → silver → gold) | **spark-iceberg container** | task = `docker exec spark-iceberg spark-submit /etl/<job>.py` (Bash/DockerOperator). Airflow *triggers*, Spark *executes* |
| compaction / maintenance | spark-iceberg container | same |

**The config-driven principle** (12-factor) is what lets dev and prod share one codebase:

```
dev  (Lenovo)            MYSQL_HOST=192.168.0.21
prod (airflow container) MYSQL_HOST=host.docker.internal
```

Same script, only the environment differs. No code change to deploy. Connection coords, credentials,
the MinIO endpoint, and the landing bucket all read from `.env` / env vars.

**Deployment mechanism.** Sync the codebase into Zeenie's mounted folders (scp/rsync, as we already do
for compose and SQL), add compose mounts (`./etl` into the spark container, the Sales code into the
airflow container), and install each container's Python deps. A `git clone` on Zeenie is a possible
later refinement; sync is sufficient now.

**Why the landing area is a MinIO bucket, not a host folder.** Two different containers touch the feed
— the extractor *writes* it, Spark *reads* it. A shared host mount would couple both containers to one
path and is brittle. Instead the feed lands in the MinIO `landing/` bucket
(`s3://landing/sales/<business_date>/`): both containers already reach MinIO over the network, there is
no mount coupling, and Spark's bronze ingestion reads it as S3 — consistent with every other layer of
the lakehouse. Object storage *is* the drop folder.

---

## 10. Bridge to the warehouse (preview)

How this source maps to the analytical star (full design in `Sales_Warehouse_Design.md`):

| OLTP source | Warehouse target | Notes |
|---|---|---|
| `customers` | `dim_customer` | **SCD-2** on `segment`, geography; surrogate `customer_key` regenerated |
| `products` + `product_categories` | `dim_product` | denormalized (category folded in); **SCD-2** on price/name |
| `stores` | `dim_store` | mostly SCD-1 |
| *(generated)* | `dim_date` | not from OLTP — generated calendar keyed by `order_date` |
| `order_items` (+ `orders`) | `fact_sales` | **grain = order line**; FK surrogate keys to dims; measures `quantity`, `unit_price`, `discount_pct`, `extended_amount` (= `line_amount`); degenerate dims `order_id`, `line_no`, `status` |

Key transformations the ETL performs (the reason the OLTP is normalized and the warehouse is not):
- **Denormalize** product+category into one `dim_product` row.
- **Conform** and **surrogate-key** dimensions (the OLTP's `customer_id` becomes a natural/business
  key; the dim gets its own `customer_key`).
- **SCD-2** via `MERGE INTO` on Iceberg — close-and-open dim rows on drift.
- **Apply fact updates** — re-emitted orders (status changes) update existing fact rows (COW/MOR).
- **Reconcile** `SUM(line_amount)` vs `order_total` as a DQ assertion.

---

## 11. Open items / future

- **Brand name** — currently `Sales` / db `sales_oltp`. Rename here if a brand emerges; it propagates
  to the lakehouse namespace and DataHub `source.app`.
- **Drop-folder location** — DECIDED: MinIO `landing/` bucket, `s3://landing/sales/<business_date>/`
  (see §9). The `landing` bucket must be created in MinIO (alongside `warehouse`).
- **Deliberate DQ defects** — optionally inject a small rate of dirty data (null geography, negative
  edge cases blocked by CHECKs, late files) to exercise warehouse DQ assertions. Off by default; the
  source stays clean until we want the lesson.
- **Volume knobs** — daily order volume, drift rates, history backfill length are all parameters of
  the generator.

---

*Runnable DDL lands in `sales_oltp_app/db/`; the generator and extractor in `sales_oltp_app/tools/`;
Airflow DAGs in `sales_oltp_app/dags/`. This document is the source-system design of record; the
warehouse and feed mechanics have their own documents.*
