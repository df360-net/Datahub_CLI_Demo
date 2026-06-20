# Sales Orchestration — Airflow DAG Design

The conductor: one Airflow DAG that runs the whole platform end to end on a daily cadence —
simulate the OLTP day, extract the feed, run the medallion ETL, gate on data quality, and refresh the
serving + BI layers. Companion docs: every other `Sales_*_Design.md` (this orchestrates them). The
tool is **Apache Airflow** (standalone, on Zeenie).

> **Scope.** This document designs *how the pipeline is orchestrated* — where each task runs, how one
> run_date threads through, idempotency/retries, and scheduling. The DAG of record is
> `lakehouse_fundamentals/docker/zeenie/airflow/dags/sales_daily.py`.

---

## 1. The chain

```
simulate_day -> extract_feed -> bronze -> silver -> gold_dims -> gold_fact -> dq_checks -> refresh_dremio -> refresh_superset
```

A single linear pipeline (each step depends on the prior). `gold_dims` precedes `gold_fact` (the fact
resolves dim keys point-in-time, so dims must be current first); `dq_checks` gates the serving refresh
(don't publish bad data). Mirrors the manual build order — now automated and repeatable.

| Task | What runs | Where |
|---|---|---|
| `simulate_day` | `generate_daily_activity.py daily --date {{ds}}` | in Airflow (Python) |
| `extract_feed` | `extract_feed.py --run-date {{ds}}` | in Airflow (Python) |
| `bronze`/`silver`/`gold_dims`/`gold_fact`/`dq_checks` | `spark-submit etl/<job>.py --run-date {{ds}}` | `docker exec spark-iceberg` |
| `refresh_dremio` | `build_dremio_views.py` (idempotent CREATE OR REPLACE) | in Airflow (Python) |
| `refresh_superset` | `setup_superset.py` (ensure dataset + metrics) | in Airflow (Python) |

---

## 2. Where tasks run — the two execution surfaces

Airflow is one container; the pipeline spans Python (DB/MinIO/HTTP) and Spark work. Rather than cram a
Spark runtime into Airflow, the DAG uses **two surfaces**:

- **Python tasks run inside Airflow.** The image adds `pymysql`, `boto3`, `faker`, `python-dotenv`,
  `cryptography` and reuses Airflow's own SQLAlchemy 1.4 — the Sales tools build engines with
  `future=True`, so 2.0-style code runs on 1.4 unchanged (no version conflict).
- **Spark tasks run in the existing `spark-iceberg` container** via `docker exec`. Airflow gets the
  Docker CLI and the host Docker socket is mounted in, so a `BashOperator` can `docker exec
  spark-iceberg spark-submit ...`. This is "Docker-out-of-Docker": Airflow drives sibling containers,
  it does not nest Docker.

> **Why not run Spark in Airflow, or everything via SSH?** Spark-in-Airflow would duplicate the engine
> and its config; SSH-to-host adds a credential surface. Docker-exec keeps each job in the container
> already built for it, with Airflow as a thin conductor. The trade-off: Airflow needs the socket
> (a privileged mount) — acceptable on a single-host lab.

---

## 3. One run_date threads through everything

Every task takes the same `{{ ds }}` (the run's logical date) as `--run-date` / `--date`. This is the
join key of the whole run:

- `simulate_day` writes orders with **business date** `{{ds}}` (audit `updated_at` = NOW, a separate
  axis).
- `extract_feed --run-date {{ds}}` pins the **landing prefix label** to `{{ds}}` (the watermark window
  is unchanged — it still pulls only `updated_at >= last_watermark`), so the feed lands at
  `landing/sales/{{ds}}/`.
- `bronze..dq` all read/write the `{{ds}}` slice.

So a single date labels the entire run's artifacts, end to end — feed prefix, bronze partition, DQ
scope — even though the incremental *content* is driven by the watermark independently.

---

## 4. Idempotency & retries

Every task is **re-runnable** (the property the whole medallion was built for): the feed MERGE-loads,
bronze `overwritePartitions`, silver/dims/fact MERGE on keys, Dremio/Superset use CREATE OR
REPLACE / get-or-create. So a failed run re-runs from the top (or from the failed task) with no
duplicates or drift. Airflow task `retries` are therefore safe. The one **non-idempotent** task is
`simulate_day` — it *inserts new orders* each run (it simulates a fresh business day); re-running it
for the same date adds more activity. That's by design (it's the data source, not an ETL step) — so on
a retry, start from `extract_feed`, not `simulate_day`.

---

## 5. Scheduling

`schedule="@daily"`, `catchup=False`, created paused (Airflow's default) so it never auto-runs
unexpectedly in the lab. Drive it by **manual trigger** (with a chosen logical date) while learning, or
unpause for a true daily cadence. `catchup=False` means no backfill storm of historical runs on
unpause. One DAG, `max_active_runs=1` (the pipeline is stateful — the watermark and the OLTP advance,
so runs must not overlap).

---

## 6. Capstone tie-in (Wave 5)

This DAG is itself **process lineage** for the DataHub capstone: the Airflow DAG and its tasks become
DataHub **DataFlow / DataJob** entities sitting on every edge of the data lineage (OLTP → landing →
bronze/silver/gold → Dremio → Superset). DataHub's Airflow plugin emits this from the running DAG.
Combined with the dataset lineage (Iceberg/Dremio) and consumption lineage (Superset), it completes
the three-dimensional picture — all published to DataHub@Murphy through the Auth Proxy.

---

## 7. Build & run order

1. Custom Airflow image (Docker CLI + Python deps); mount the Docker socket + the Sales code at
   `/opt/sales` + a combined `.env`.
2. Deploy the Sales code + `dq_checks.py` to Zeenie; deploy the DAG to `airflow/dags/`.
3. Verify Airflow can reach the Docker socket (`docker ps`), then trigger one run for a fresh date and
   watch it go green task by task.

---

*DAG of record: `lakehouse_fundamentals/docker/zeenie/airflow/dags/sales_daily.py`. Image + mounts:
the Zeenie compose. This document is the orchestration design of record.*
