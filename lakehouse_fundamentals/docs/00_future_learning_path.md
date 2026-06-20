# Future Learning Path — Building the Modern Analytics Stack

A wave-by-wave roadmap for standing up the full modern data-analytics stack on the **Dell**
(32 GB, incoming) and *learning* each piece as you add it — not collecting tools, but
understanding their jobs.

> **The one rule: install in purposeful waves, not all at once.** The modern-stack trap is
> accumulating ten services that sit idle while you learn nothing. 32 GB can't run Spark +
> Trino + Flink + Kafka + Airflow hot together anyway. Each wave earns its place before the
> next begins. The skill isn't "I installed it" — it's "I know which tool to reach for and why."

This doc is the *roadmap*; the real learning happens by building each wave and discussing what
we see — same Concept → Do → See → Check rhythm as the
[Lakehouse_Learning_guide.md](Lakehouse_Learning_guide.md). It builds on what you already know:
the four layers ([Lakehouse.md](Lakehouse.md)), object storage
([01_understanding_S3.md](01_understanding_S3.md)), and Parquet
([02_understanding_Parquet.md](02_understanding_Parquet.md)).

---

## The target topology (post-Dell)

Three machines on the LAN, each a clear role:

| Machine | Role |
|---|---|
| **Lenovo** (8C/16T, 16GB) | control plane — VSCode, git, DuckDB, dbt CLI. Drives the others. |
| **Murphy** (6C/12T, 16GB) | DataHub + MSSQL (DCF_DB). The governance/catalog + OLTP source. |
| **Dell** "Zeenie" (10C/12T, **32GB**, reserved IP `192.168.0.21`) | **the lakehouse powerhouse** — this stack lives here. |

The payoff: a genuinely *distributed* lakehouse. DataHub on Murphy ingests the Iceberg catalog
on the Dell over the LAN; engines on the Lenovo or Dell query the same storage. Decoupled
storage and compute, made physical across three boxes.

## The stack at a glance

| Tier | Components | Role | Wave |
|---|---|---|---|
| **0 — substrate** (always on) | MinIO (S3), Iceberg REST catalog, Parquet | storage + table format | 0 |
| **1 — engines** | Spark (write/ETL), Trino (query/federation), DuckDB (local) | compute | 1 |
| **2 — transform** | **dbt** | version-controlled SQL transformations | 2 |
| **3 — orchestrate** | Airflow or Dagster | code-defined scheduling | 3 |
| **4 — streaming** | Kafka + Flink | continuous ingestion into Iceberg | 4 |
| **capstone** | DataHub ingest, catalog upgrade | governance + lineage end-to-end | 5 |

Run subsets via **docker-compose profiles**: Tier 0+1 up by default; bring up streaming only
when you're studying it.

---

## Wave 0 — Re-establish the substrate on the Dell

**Concept.** The lakehouse floor: MinIO (object store) + Iceberg REST catalog + Spark, the same
stack you already ran on Murphy. Parquet is a library, nothing to install.
**Do.** Port the Murphy `docker-compose.yml` to the Dell; bring up MinIO + iceberg-rest +
spark-iceberg. Re-create `demo.smoke.*` to confirm the floor works.
**Bridge.** This is the storage layer you now understand cold — object store under table format
under file format. Nothing new; it's your launchpad with real RAM behind it.
**Done when.** Spark writes an Iceberg table to MinIO and reads it back; DuckDB on the Lenovo
queries it over the LAN.

## Wave 1 — Query engines: add Trino

**Concept.** The open MPP SQL query engine — coordinator + workers, the truest open analog of
Teradata. Point it at the *same* Iceberg tables Spark wrote.
**Do.** Add Trino (coordinator + a worker) to the compose; wire it to the Iceberg REST catalog;
run a query, then `EXPLAIN` it and read the distributed plan.
**Bridge.** Your Teradata MPP instincts transfer directly: coordinator ≈ Parsing Engine, worker
≈ AMP, network shuffle ≈ BYNET redistribution. Skew and join strategy reasoning carry over.
**Done when.** The same table reads identically from Spark, Trino, and DuckDB — three engines,
one copy of data. (You've now *lived* the engine-disposable thesis.)

## Wave 2 — Transformation: dbt  ← your highest-leverage tool

**Concept.** dbt turns SQL `SELECT`s into version-controlled, tested, documented, dependency-
ordered transformations. It is the connective tissue of modern analytics — and it sits *on top*
of DuckDB / Trino / Spark (engine-agnostic; the decoupling theme again).
**Do.** Install dbt with `dbt-duckdb` (fast local) or `dbt-trino`. Rebuild a **bronze → silver →
gold** medallion warehouse as dbt models — and make the gold layer a real **star schema**.
Add `tests:` (unique, not_null, relationships) and generate the docs/lineage graph.
**Bridge.** This is where your dimensional-modeling mastery becomes *modern* practice. Same star
schemas, same grain/conformed-dimension discipline you know cold — now as code, tested, and
lineage-tracked. The biggest single return on your existing skill.
**Done when.** `dbt run` builds your medallion star schema end-to-end and `dbt test` passes;
`dbt docs` shows the model lineage.

## Wave 3 — Orchestration: Airflow (or Dagster)

**Concept.** Code-defined scheduling and dependency management for your jobs — the modern,
git-versioned version of a job scheduler.
**Do.** Add Airflow (or Dagster — more modern, asset-oriented). Build one DAG: ingest → `dbt
run` → `dbt test` → a Spark maintenance job (compaction). Schedule it; watch a run.
**Bridge.** This is your Teradata BTEQ scripts + scheduler world, rebuilt as code: dependencies,
retries, backfills, observability — but declarative and version-controlled.
**Done when.** A scheduled DAG runs your daily ingest→transform→test→maintain pipeline unattended.

## Wave 4 — Streaming: Kafka + Flink  ← your real catch-up frontier

**Concept.** Continuous processing: data never stops arriving. Kafka is the durable event log;
Flink is the stream processor that writes into Iceberg in near-real-time.
**Do.** Add Kafka + Flink (their own compose profile — RAM-heavy). Produce events to a Kafka
topic; run a Flink job that streams them into an Iceberg table; query that table from Trino
*while it's still updating*.
**Bridge — and the honest gap.** This is the **deepest mindset shift** from your batch-oriented
RDBMS/Teradata background: from "load, then query" to "the data is a continuous flow." There's
no clean Teradata analog — this is genuinely new territory. Spend the most time here; it's where
"catching up" actually means learning something, not re-mapping what you know.
**Done when.** Events flow Kafka → Flink → Iceberg, and a Trino query against the table returns
fresher results each time you run it.

## Wave 5 — Capstone: close the governance loop

**Concept.** Everything wires back to DataHub (which you already know). An Iceberg table is just
a `dataset` entity.
**Do.** Point a DataHub Iceberg ingestion recipe (on Murphy) at the Dell's REST catalog over the
LAN; ingest schema + lineage. Optionally upgrade the catalog from `iceberg-rest` to **Nessie**
(git-like, branchable) or **Apache Polaris** to see a production-grade catalog.
**Bridge.** Ties the DataHub track to the lakehouse track — pull-based ingestion vs the
push-based CardCompass publisher you built. Same URN/aspect machinery.
**Done when.** Your Dell Iceberg tables appear in DataHub on Murphy, with lineage — a distributed
governance loop across three machines.

---

## Where you're already ahead vs. the real gaps

Don't spend energy re-learning what you know cold. Spend it on the genuinely new.

| Already strong (transfers directly) | The real catch-up (new paradigms) |
|---|---|
| SQL, dimensional modeling (→ dbt) | **Streaming / continuous processing** (Kafka + Flink) |
| Query tuning, stats, skew (→ Trino) | Orchestration-as-code (Airflow/Dagster) |
| MPP execution (→ Trino coordinator/workers) | Immutable-files / decoupled mechanics (in progress) |
| Star schemas, grain, conformed dims (→ gold layer) | Engine-agnostic, format-as-franchise thinking |

The pattern: your **modeling and tuning** intuition is an asset to lean on; the **operational
paradigm** (streaming, code-defined pipelines, decoupled immutable storage) is the part to
build new. You're not a beginner catching up — you're an expert porting deep skills onto a new
substrate, with one genuinely new chapter (streaming).

## Practical notes

- **RAM budget (32 GB).** Spark's JVM, and Kafka + Flink, are the hogs. Never run Spark *and*
  Flink *and* Kafka *and* Trino all hot at once. Use compose **profiles** to bring up only the
  tier you're working in.
- **One compose, many profiles.** Tier 0+1 = default; `--profile transform`, `--profile stream`
  for the rest. Keeps the substrate stable while you add/remove higher tiers.
- **Same Docker-Desktop-over-SSH gotcha** as Murphy applies to the Dell — image *pulls* need the
  interactive console session (Scheduled Task workaround). Daily start/stop/exec is fine over SSH.

## On-arrival checklist

- [ ] Run `powercfg /batteryreport` within the **30-day return window** (battery health unverified).
- [ ] Confirm specs (i5-1245U, 10C/12T, 32 GB, 512 GB NVMe).
- [x] **Reserved `192.168.0.21`** via NETGEAR DHCP Address Reservation, bound to the Dell's Wi-Fi MAC `00-A5-54-4A-4E-5D` (the address it already held — no renew needed).
- [ ] Install Docker Desktop; set up SSH (key auth) + a remote Docker context from the Lenovo.
- [ ] Open Windows Firewall for the LAN (scoped to `192.168.0.0/24`) on the ports each tier needs.

## Progress tracker

- [x] **Wave 0** — Substrate on the Dell (MinIO + Iceberg + Spark) — *live on Zeenie; verified DuckDB@Lenovo read Spark@Zeenie's Iceberg table over the LAN*
- [ ] **Wave 1** — Trino (three engines, one copy)
- [ ] **Wave 2** — dbt (medallion star schema, tested)
- [ ] **Wave 3** — Orchestration (one scheduled pipeline)
- [ ] **Wave 4** — Kafka + Flink (streaming into Iceberg)
- [ ] **Wave 5** — Capstone (DataHub ingest, distributed governance loop)

Tell me a wave number when the Dell is ready, and we build it together.
