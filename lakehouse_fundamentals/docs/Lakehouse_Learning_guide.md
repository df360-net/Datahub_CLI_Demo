# Lakehouse Learning Guide

A step-by-step, hands-on path to understanding the lakehouse — learned against the live
Iceberg + Spark + MinIO sandbox on Murphy. We work through this **together**: the guide is
the roadmap; the real learning happens by running things and discussing what we see.

**Prerequisites**
- The sandbox is up (see [README.md](README.md)). Quick check: all four endpoints return
  200, Jupyter reachable at http://192.168.0.16:8888.
- You already know the static model — [Lakehouse.md](Lakehouse.md) (the four layers) and
  why Iceberg is an open table format. This guide makes that model *tangible*.

**How we'll use each module** (same rhythm that worked for DataHub):
1. **Concept** — the idea, briefly. We go deeper live.
2. **Do it** — you run the snippet in Jupyter / `spark-sql` / the MinIO console.
3. **See it** — look at what actually changed (files in MinIO, snapshot rows).
4. **Check** — one question to confirm it landed before moving on.

Orientation: the Spark catalog is **`demo`**, backed by the `iceberg-rest` catalog, whose
warehouse is `s3://warehouse/` on MinIO. So a table `demo.<ns>.<table>` = Iceberg metadata
in the REST catalog **plus** Parquet files under `warehouse/` in MinIO. We already created
`demo.smoke.t` as a smoke test — we'll use it and build more.

---

## Part A — Make the four layers tangible

### Module 1 — Anatomy of an Iceberg table: the pointer chain
**Concept.** A table isn't a file — it's a *chain of metadata* ending in data files:
`catalog -> metadata.json -> manifest list -> manifest files -> data files (Parquet)`.
This is the lakehouse equivalent of DataHub's "URN -> aspect" atom: the structural unit
everything else builds on.

**Do it / See it.** In the MinIO console (http://192.168.0.16:9001, `admin`/`password`)
open `warehouse/smoke/t/`. Note the two folders: `data/` (Parquet) and `metadata/`
(`*.metadata.json`, `snap-*.avro` manifest lists, `*.avro` manifests). Then in Jupyter:
```python
spark.sql("SELECT * FROM demo.smoke.t.files").show(truncate=False)      # the data files
spark.sql("SELECT * FROM demo.smoke.t.manifests").show(truncate=False)  # the manifests
```
**Check.** When you `INSERT` one row, which layers of the chain get a *new* file, and which
just get re-pointed?

### Module 2 — Why columnar? Parquet up close
**Concept.** Parquet stores data **by column**, in row groups, with per-column compression
and min/max stats. That's *why* analytics is fast: a query that touches one column reads
only that column's bytes, and stats let it skip whole row groups. This is the row-vs-columnar
"why" behind everything (and why MSSQL, a row store, is slow at scans).

**Do it.** Create a wider table and compare reading one column vs all:
```python
spark.sql("CREATE TABLE demo.smoke.wide USING iceberg AS "
          "SELECT id, id*2 AS a, id*3 AS b, cast(id AS string) AS label "
          "FROM range(1000000)")
spark.sql("SELECT sum(a) FROM demo.smoke.wide").show()   # reads ~1 column
```
**See it.** Open the Spark UI (http://192.168.0.16:8082) and look at the bytes read for the
column-only query vs `SELECT *`. **Check.** Why does column pruning help a warehouse query
but barely help an OLTP "fetch one whole order" query?

---

## Part B — What the table format buys you

### Module 3 — Snapshots & time-travel
**Concept.** Every write creates an immutable **snapshot**; the table is just a pointer to
the current one. You can query the past and roll back.
**Do it.**
```python
spark.sql("INSERT INTO demo.smoke.t VALUES (3,'third write')")
spark.sql("SELECT * FROM demo.smoke.t.snapshots").show(truncate=False)
spark.sql("SELECT * FROM demo.smoke.t.history").show(truncate=False)
# time-travel to the first snapshot id you saw:
spark.sql("SELECT * FROM demo.smoke.t VERSION AS OF <snapshot_id>").show()
```
**Check.** How is an Iceberg snapshot like a DataHub *timeseries* aspect, and how is the
"current pointer" like a *versioned* aspect?

### Module 4 — Schema evolution (and why it's safe)
**Concept.** Add / rename / drop / reorder columns with no data rewrite. Safe because
Iceberg tracks columns by **field ID**, not by name or position.
**Do it.**
```python
spark.sql("ALTER TABLE demo.smoke.t ADD COLUMN amount double")
spark.sql("ALTER TABLE demo.smoke.t RENAME COLUMN msg TO message")
spark.sql("INSERT INTO demo.smoke.t VALUES (4,'with amount',99.5)")
spark.sql("SELECT * FROM demo.smoke.t ORDER BY id").show()
```
**Check.** Old data files don't have `amount`. Why does the old data still read back
correctly (what does Iceberg return for the missing column, and how does it know)?

### Module 5 — ACID, upserts, and copy-on-write vs merge-on-read
**Concept.** Writes are atomic via an optimistic swap of the metadata pointer. Row-level
changes come in two flavors: **copy-on-write** (rewrite affected files) vs **merge-on-read**
(write delete files, merge at read). This is the lakehouse answer to "how do I UPDATE/DELETE
in a world of immutable files."
**Do it.**
```python
spark.sql("""
  MERGE INTO demo.smoke.t t USING (SELECT 1 AS id, 'updated!' AS message) s
  ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET t.message = s.message
  WHEN NOT MATCHED THEN INSERT *
""")
spark.sql("SELECT * FROM demo.smoke.t ORDER BY id").show()
```
**Check.** After the MERGE, look at `.snapshots` — what operation type did it record, and how
many new data files appeared?

### Module 6 — Partitioning & file pruning
**Concept.** Partitioning splits data into separate files by a column so queries skip
irrelevant files. Iceberg does **hidden partitioning** — you partition by `days(ts)` without
adding a partition column to the schema or to your queries.
**Do it.**
```python
spark.sql("CREATE TABLE demo.smoke.events (id bigint, ts timestamp, val double) "
          "USING iceberg PARTITIONED BY (days(ts))")
# insert a few rows across different days, then:
spark.sql("SELECT * FROM demo.smoke.events WHERE ts > '2026-06-13'").explain()
```
**Check.** In the plan, what tells you Iceberg pruned partitions *before* reading data —
and where did the pruning decision come from (which layer from Module 1)?

---

## Part C — Engine, operations, and integration

### Module 7 — Engine-agnostic: same table, different engine
**Concept.** The payoff of "open": the table isn't owned by Spark. Another engine reads the
same files. This is the no-lock-in property made real.
**Do it.** Read the Iceberg table from **DuckDB** (or Trino) pointed at the same MinIO +
REST catalog, and confirm identical rows. (We'll set this up together — it proves the
storage layer is the durable asset and the engine is disposable.)
**Check.** Why can two engines safely *read* concurrently, and what has to be true for them
to safely *write* concurrently (tie back to Module 5)?

### Module 8 — Table maintenance (the housekeeping a lakehouse needs)
**Concept.** Streaming/incremental writes create many small files and pile up old snapshots.
A lakehouse needs maintenance: **compaction** (rewrite small files), **expire snapshots**,
**remove orphan files**. Without it, read performance and storage degrade.
**Do it.**
```python
spark.sql("CALL demo.system.rewrite_data_files('smoke.t')")
spark.sql("CALL demo.system.expire_snapshots('smoke.t', TIMESTAMP '2026-06-14 00:00:00')")
spark.sql("CALL demo.system.remove_orphan_files(table => 'smoke.t')")
```
**Check.** After `expire_snapshots`, what can you no longer do that you could in Module 3 —
and what's the tradeoff you're making?

### Module 9 — Close the loop: catalog Iceberg into DataHub
**Concept.** Everything you learned about DataHub applies here: an Iceberg table is just a
`dataset` entity. DataHub has an Iceberg ingestion source that reads the REST catalog and
publishes schema (and lineage) — the same URN/aspect/MCL machinery from
[../../dh_fundamentals/DataHub_fundamentals.md](../../dh_fundamentals/DataHub_fundamentals.md).
**Do it.** (Requires DataHub running — we run one stack at a time on Murphy, so we'll plan
the swap.) Point a DataHub Iceberg recipe at `http://...:8181`, ingest, and find
`demo.smoke.t` in the DataHub UI as a dataset.
**Check.** Your CardCompass tables and these Iceberg tables both become `dataset` URNs in
DataHub. What's *different* about where their schema metadata comes from (pull vs push)?

---

## Progress

- [x] Module 1 — Anatomy of an Iceberg table
- [x] Module 2 — Why columnar? Parquet up close
- [x] Module 3 — Snapshots & time-travel
- [ ] Module 4 — Schema evolution
- [ ] Module 5 — ACID, upserts, COW vs MOR
- [ ] Module 6 — Partitioning & file pruning
- [ ] Module 7 — Engine-agnostic (DuckDB/Trino)
- [ ] Module 8 — Table maintenance
- [ ] Module 9 — Catalog Iceberg into DataHub

## Recommended pace

Part A in one sitting (it makes the abstract concrete fast). Part B is the heart — one
module per sitting, lots of looking at snapshots and files. Part C ties it back to the
DataHub track you already know. Tell me a module number and we start.
