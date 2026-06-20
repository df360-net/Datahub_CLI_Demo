# Sales BI — Apache Superset Design

The presentation/consumption tier: dashboards and charts for business users, built on the Dremio
`sales_curated` semantic layer. Companion docs: `Sales_Serving_Design.md` (the views Superset reads)
and `Sales_Warehouse_Design.md` (the star underneath). The tool is **Apache Superset**.

> **Scope.** This document designs *how Superset is deployed, connected, and developed* — the lean
> container, the Dremio Flight connection, the dataset/dashboard model, and the dashboards-as-code
> workflow. Build artifacts: `lakehouse_fundamentals/docker/zeenie/superset/` (image + config) and
> `sales_oltp_app/bi/` (API automation).

---

## 1. Why Superset — and the Teradata bridge

Superset is the open-source BI layer. It was chosen over Metabase for one decisive reason:
**AI-developability**. Superset assets (database connections, datasets, charts, dashboards) are
addressable through a full **REST API** and export/import as **YAML**, so Claude can author and
version them in git — dashboards become code, not clicks. Metabase is GUI-first with serialization
behind its paid tier.

| BI concern | Superset | **Teradata-world bridge** |
|---|---|---|
| Semantic source | Dremio datasets (SQL) | your reporting views / BI semantic layer |
| Metric definitions | dataset metrics + saved charts | report definitions / KPIs |
| Distribution | dashboards in the browser | BI portal reports |
| Automation | REST API + YAML import/export | BI tool's CLI / repository import |

> You've built reporting layers for years; Superset is that layer, but **API-driven and
> git-versioned**. The discipline carries over (curated datasets, conformed metrics); what's new is
> that the whole BI catalog is code.

---

## 2. Deployment — lean single container (mirrors Airflow standalone)

One container, the simplest thing that works — the same philosophy as the Airflow-standalone setup:

- Image: **`apache/superset`** + **`sqlalchemy-dremio`** baked in (custom one-line Dockerfile —
  `superset/Dockerfile`). The Dremio driver is not in the base image.
- Metadata DB: **SQLite** at `/app/superset_home/superset.db`, persisted on a named volume
  (`superset-home`). No external Postgres — this is a learning/lab instance, single-user.
- Runs on the shared `iceberg_net` so it reaches Dremio as `dremio:32010` directly.
- Bootstrap on start: `superset db upgrade` → `fab create-admin` → `superset init` → launch server.
- UI: **http://192.168.0.21:8088**, admin `admin` / `password` (lab creds).

> **Not the official multi-container compose.** That ships Redis + Celery + Postgres for concurrency
> and async queries — overkill here. We scale up later only if dashboards need async/cached queries
> (then add Redis + a real metadata DB), exactly as Airflow steps up to LocalExecutor when needed.

---

## 3. Connection to Dremio — Arrow Flight SQL

Superset → Dremio uses the **`dremio+flight://`** dialect (Arrow Flight SQL on port **32010**) — pure
Python (pyarrow), no ODBC driver to install. SQLAlchemy URI:

```
dremio+flight://<user>:<pwd>@dremio:32010/dremio?UseEncryption=false
```

- `UseEncryption=false` — Dremio OSS serves Flight without TLS in the lab.
- Auth is a **Dremio** user (not the MySQL user); the lab uses the Dremio admin. A dedicated read-only
  Dremio BI user is the future hardening (least-privilege for the BI tool).
- Host `dremio` resolves on `iceberg_net`; from outside the network it would be `192.168.0.21:32010`.

> The alternative `dremio://` (ODBC) dialect needs Dremio's ODBC driver in the image — heavier and
> platform-specific. Flight is the modern, dependency-light path.

---

## 4. The dataset & dashboard model

- **Primary dataset:** `sales_curated.vw_sales_report` — the wide, as-of one-big-table. Superset
  registers it as a physical dataset; charts slice it with no join configuration (the join is already
  baked into the Dremio view). This is the dataset business users build on.
- **Star datasets (optional):** the `dim_*` + `fact_sales` views, for power users who want to define
  their own joins/metrics in Superset's dataset layer.
- **Conformed metrics** on the report dataset: `revenue = SUM(extended_amount)`,
  `units = SUM(quantity)`, `orders = COUNT(DISTINCT order_id)`, `avg_line = AVG(extended_amount)`.
- **First dashboard — "Sales Overview":** revenue trend by month, revenue by segment, revenue by
  category, revenue by region/channel, top products — all from `vw_sales_report`. As-of semantics mean
  a historical month reflects the dimension state at that time.

---

## 5. Dashboards as code

The payoff of choosing Superset:

- **Provision** the database connection + datasets via the REST API (`bi/setup_superset.py`):
  `POST /api/v1/security/login` → `POST /api/v1/database/` (the Dremio connection) →
  `POST /api/v1/dataset/` (register `vw_sales_report` + metrics). Idempotent and re-runnable.
- **Export** finished dashboards as a YAML bundle (`GET /api/v1/dashboard/export`) into
  `sales_oltp_app/bi/assets/` — git-tracked, reviewable, diffable.
- **Re-import** the bundle into any fresh Superset (`POST /api/v1/dashboard/import`) — the BI layer is
  reproducible from git, not trapped in one instance's SQLite file.

> This is why Superset, concretely: the entire BI catalog round-trips through git. A dashboard change
> is a reviewable diff, and a rebuild is a script — same "code, not clicks" standard as the Dremio
> serving layer.

---

## 6. Security & access

- Lab creds only, kept simple (admin/password) — single-user learning instance.
- Superset reads Dremio read-only; it never writes the lake or the warehouse.
- `SUPERSET_SECRET_KEY` is set via env (stable, or sessions break) — lab-only value, not a real secret.
- Hardening path (later): dedicated read-only Dremio BI user; real `SECRET_KEY`; Postgres metadata.

---

## 7. Lineage tie-in (capstone, Wave 5)

Superset is the **consumption** end of the end-to-end lineage published to DataHub@Murphy: each chart
and dashboard becomes a DataHub **Chart/Dashboard** entity, linked to the `vw_sales_report` dataset it
reads — DataHub's Superset ingestion connector harvests this. That closes the chain
`MySQL → landing → bronze/silver/gold → Dremio → Superset`, the full data + process + consumption
lineage. Last brick; only after the dashboards are stable.

---

## 8. Build & run order

1. Build the image + bring up the container (`docker compose up -d superset`); verify UI + login.
2. Provision Dremio connection + `vw_sales_report` dataset via `bi/setup_superset.py`.
3. Build the "Sales Overview" dashboard; export its YAML to `bi/assets/`.
4. (Capstone) DataHub Superset ingestion.

Each step verifies before the next — same discipline as the medallion build.

---

*Container artifacts: `lakehouse_fundamentals/docker/zeenie/superset/`. BI automation + exported
assets: `sales_oltp_app/bi/`. This document is the BI design of record.*
