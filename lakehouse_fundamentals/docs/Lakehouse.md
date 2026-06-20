# The Lakehouse: Four Layers

A **lakehouse** fuses two worlds: the cheap, open storage of a **data lake** with the
transactions and schema guarantees of a data **warehouse**. You get warehouse behavior
*directly on* lake storage — no separate warehouse to copy data into.

The whole picture is built from four stacked layers:

```
Engine        Spark / Trino / Flink / DuckDB / Snowflake   ─┐  compute
   ↑                                                        │
Iceberg       table format (ACID, schema, snapshots)        ─┤
Data files    Parquet / ORC (columnar)                      ├─ the OPEN STORAGE LAYER
Object store  S3 / ADLS / GCS / MinIO                       ─┘
                                                          
        the whole picture = LAKEHOUSE ARCHITECTURE
```

The golden rule: **each layer stores nothing the layer below already stores.** Bytes live
at the bottom; every layer above only adds *organization and meaning*.

---

## Layer 1 (bottom) — Object storage: where the bytes live

**Examples:** Amazon S3, Azure ADLS, Google Cloud Storage, MinIO (self-hosted).

The cheap, durable, near-infinite bottom layer. It stores **blobs** — opaque files — and
nothing more. It knows nothing about tables, schemas, or rows; it just holds bytes by key.
This is what makes the lakehouse economical: storage is commodity object storage, not
expensive proprietary database disk.

## Layer 2 — Data files: how the bytes are laid out

**Examples:** Parquet, ORC (columnar) · Avro (row-oriented).

The actual rows of data, encoded into files. In analytics these are almost always
**columnar** (Parquet/ORC): each column is stored together, so a query that aggregates one
column reads only that column's bytes and compresses them heavily. This is *why* the
lakehouse is fast for analytics. Still just files, though — a folder full of Parquet has no
notion of "this is one table" or "what's the current version."

## Layer 3 — Table format: the abstraction that makes files a table

**Examples:** Apache Iceberg, Delta Lake, Apache Hudi.

This is the layer that turns a scattered pile of Parquet files into one coherent,
versioned **table**. It stores **no data of its own** — only **metadata**:

- which files currently belong to the table,
- the schema (and its history, so columns can be added/renamed safely),
- **snapshots** for time-travel (query the table as of an earlier version),
- per-file statistics that let engines skip files they don't need.

What it buys you over raw files: **ACID transactions, time-travel, and safe schema /
partition evolution** — database guarantees, on plain files in cheap storage.

Iceberg's own anatomy: a **catalog** (points to each table's current metadata) →
**metadata files** (schema, snapshot list) → **manifests** (which data files are in each
snapshot) → **data files** (the Parquet from Layer 2).

## Layer 4 (top) — Engine: who reads and writes

**Examples:** Spark, Trino, Flink, DuckDB, Snowflake.

The compute that actually executes queries and transformations. **The engine owns none of
the storage** — it sits on top and reads/writes through the table format. Because the
storage layer is open, engines are **interchangeable**: run Spark for heavy ETL, Trino for
ad-hoc SQL, DuckDB on a laptop — all hitting the **same one copy** of the data.

---

## The two ideas that make it "a lakehouse"

**1. Decoupled (disaggregated) storage and compute.** Storage is just open files plus
metadata, so compute is interchangeable and disposable while storage is the durable, open
asset. A classic warehouse (e.g. Snowflake's native format) welds the two together; the
lakehouse pries them apart. This is the defining architectural property — the phrase to use
in a design discussion.

**2. Open, no lock-in.** Open file format (Parquet) + open table format (Iceberg) means any
engine can read the same table. One copy of data, many engines, no vendor owning your data.
This is the usual reason a firm picks **Iceberg** specifically — it is the vendor-neutral
table format (Delta is the same category but historically Databricks-centric).

## What this is *not*

This four-layer picture is the **lakehouse architecture** — the storage-and-engine
foundation. It is **not** the same as the "**modern data stack**," which is broader: it
includes ingestion (e.g. Fivetran), transformation (dbt), orchestration (Airflow), and BI
(Looker) layered *on top*. The lakehouse is the foundation; the modern data stack is the
whole city built on it.

## One-line summary

> A **lakehouse** is warehouse guarantees (ACID, schema, time-travel) delivered on lake
> economics (open columnar files on cheap object storage), with **storage and compute
> decoupled** so any engine can read one open copy of the data.
