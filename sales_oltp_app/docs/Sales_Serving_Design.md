# Sales Serving — Dremio Semantic Layer Design

The consumption layer of the pipeline: it sits between the physical lakehouse (Iceberg tables in
Nessie) and BI (Superset), and exposes a **governed, business-friendly view of the gold star**.
Companion docs: `Sales_Warehouse_Design.md` (the star this serves) and the Superset doc (next).
The engine is **Dremio OSS**; it reads the same Nessie catalog Spark writes — one catalog, two
engines (Spark = writer/ELT, Dremio = reader/serving).

> **Scope.** This document designs *what Dremio exposes and how* — the curated space, the views, the
> all-important surrogate-key join, and acceleration. The DDL of record is `serving/dremio_views.sql`;
> it is applied idempotently by `serving/build_dremio_views.py` via Dremio's REST API.

---

## 1. Role of the serving layer — and the Teradata bridge

Dremio is the **query + semantic layer**. It decouples consumers from physical storage paths, presents
clean business datasets, federates across sources, and accelerates queries with **Reflections**
(transparent materializations). BI tools never touch Iceberg/Parquet directly — they query Dremio.

| Serving concern | Dremio | **Teradata bridge** |
|---|---|---|
| Business-friendly model | curated **Space** of views (VDS) | semantic-layer / presentation **views** |
| Physical/logical separation | views over the `nessie` source | views over base tables |
| Query acceleration | **Reflections** (raw + aggregation) | join/aggregate **JIs**, cached results |
| Federation | one engine over many sources | (n/a — single system) |

> The shape is your Teradata semantic views: consumers bind to stable view names, the physical model
> can change underneath. What's new: Dremio is its own **distributed query engine** (not the storage
> engine), and acceleration is a *Reflection* it maintains, not an index you design.

---

## 2. What we expose — gold only, in a curated Space

- **Only the gold star is served.** `sales_bronze` and `sales_silver` are ETL internals — they stay in
  the `nessie` source but are never surfaced to BI. Consumers see only the `sales_curated` space.
- A Dremio **Space `sales_curated`** holds the semantic layer:
  - thin pass-through views of the star — `dim_customer`, `dim_product`, `dim_store`, `dim_date`,
    `fact_sales` — for flexible, self-service dimensional analysis;
  - one wide reporting view — `vw_sales_report` — a denormalized one-big-table for Superset.
- BI connects to **`sales_curated` only**. This is the governance boundary: rename/refactor physical gold
  without breaking dashboards, and keep raw layers out of consumers' reach.

---

## 3. The crux — the surrogate-key join gives "as-of" for free

`fact_sales` already carries **point-in-time-resolved surrogate keys** (`customer_key`, `product_key`,
`store_key` were resolved at load time to the dimension version valid at `order_date` — see warehouse
design sec 7.2). The serving layer reaps that:

```sql
-- AS-OF (default): join on the SURROGATE key -> the dim version current at the transaction
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key   -- plain equi-join, no temporal predicate
```

A plain equi-join on the surrogate key returns the customer's segment **as it was when they ordered** —
no `valid_from`/`valid_to` range logic, no `is_current` filter, at query time. This is the entire payoff
of building SCD-2 + point-in-time facts: the temporal correctness is baked into the key, so BI stays
simple and still gets historical truth (a March order shows the March segment even if the customer
upgraded in June).

> **As-of vs current-state.** `vw_sales_report` uses the as-of join (surrogate key) — correct for
> transactional reporting. If a "slice all history by the customer's *current* segment" view is ever
> needed, that's a different join — on the **business key** filtered to `is_current = true` — and would
> be its own view (`vw_sales_report_current`). Not built yet; add on demand.

---

## 4. The views (`serving/dremio_views.sql`)

| View (in `sales_curated`) | Definition | Purpose |
|---|---|---|
| `dim_customer` | `SELECT * FROM nessie.sales_gold.dim_customer` | star dim, all versions + SCD-2 control cols |
| `dim_product` | pass-through | star dim |
| `dim_store` | pass-through | star dim |
| `dim_date` | pass-through | calendar |
| `fact_sales` | pass-through | the fact, resolved surrogate keys |
| `vw_sales_report` | `fact_sales` ⨝ all dims **on surrogate keys** | wide OBT for Superset (as-of) |

`vw_sales_report` references the curated `sales_curated.*` views (not the physical paths), so the space is
self-contained and edits to a dim view flow through to the report. It surfaces business attributes
(segment, category, channel, region, calendar parts) alongside the additive measures (`quantity`,
`extended_amount`), and distinguishes `product_list_price` (dim, as-of list price) from
`sale_unit_price` (fact, the actual price charged on the line).

---

## 5. Reflections — acceleration (future, not yet built)

Once dashboards exist, add Dremio **Reflections** to make them fast without changing queries:
- a **raw reflection** on `fact_sales` (columnar, partitioned by date) for drill-downs;
- **aggregation reflections** on `vw_sales_report` over the common group-bys (date × segment × region ×
  category) for dashboard tiles.

Reflections are Dremio-maintained Iceberg materializations in its own store; the optimizer rewrites
queries to hit them transparently. Defer until we see real Superset query patterns — premature
reflections are wasted maintenance (same discipline as not over-indexing in Teradata).

---

## 6. Security & access

- **Serving is read-only.** Dremio reads gold; it never writes to the lake.
- The optional **`SALES_OLTP` source** (Dremio → MySQL, for source-to-target lineage exploration) must
  connect as the read-only **`sales_extract`** user, scoped to `sales_oltp` — this both enforces
  least-privilege and hides MySQL's `performance_schema` from the catalog tree (a privileged user sees
  it; a scoped user does not).
- Dremio admin credentials live in the gitignored `.env` (`DREMIO_ENDPOINT/USER/PWD`) — never in code.

---

## 7. Reproducibility

The serving layer is **code, not clicks**: `serving/dremio_views.sql` is the DDL of record (git-tracked,
no secrets), applied by `serving/build_dremio_views.py` against Dremio's REST API
(`POST /apiv2/login` → ensure `sales_curated` space → `CREATE OR REPLACE VIEW` per statement, polled to
completion). Re-runnable: `CREATE OR REPLACE` makes every view idempotent, so the script is safe to
re-run after any star change and is a natural Airflow task in the daily DAG (step 7).

---

## 8. Lineage tie-in (capstone, Wave 5)

Each Dremio view is a node in the end-to-end lineage published to DataHub@Murphy: the chain
`silver → gold Iceberg → Dremio VDS → Superset chart` becomes **dataset lineage** (with column-level
`fineGrainedLineages` where the wide view maps fact/dim columns through). Dremio exposes view
definitions via its API/INFORMATION_SCHEMA, which the lineage emitter parses. Last brick — only after
steps 1-6 emit stable URNs.

---

*Serving DDL: `serving/dremio_views.sql`. Builder: `serving/build_dremio_views.py`. This document is the
serving design of record; BI (Superset) has its own doc.*
