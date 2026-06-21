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

---

## Part D — Dimensional modeling on the lakehouse (SCD-2 star)

Worked live against the `sales_oltp_app` gold layer (`dim_customer`, `dim_product`,
`dim_store`, `dim_date`, `fact_sales`), not the `smoke` sandbox. The *modeling* here is pure
Kimball — unchanged from a Teradata warehouse. What changed is the substrate underneath. That
contrast is the whole point of this part.

### Module 10 — Two orthogonal "as of" axes
**Concept.** "As of a date" means two completely different things, and conflating them is the
classic trap:
- **Business as-of (Axis 2)** — *when was this true in the business.* Lives in plain
  `valid_from` / `valid_to` columns (SCD-2). Queried with an ordinary `WHERE` / join predicate.
  No special syntax. This is the Teradata `PERIOD` temporal join, just spelled out.
- **Physical time-travel (Axis 1)** — *when did this row land in the table.* An Iceberg/Nessie
  feature: `... AT TIMESTAMP '2026-01-01 00:00:00'` (also `AT SNAPSHOT`, `AT COMMIT`,
  `AT BRANCH/TAG`). This is **new** — Teradata never had a commit-time axis.

**Check.** "Give me `fact_sales` as of 2026-01-01" — which axis? (Usually Axis 2. And note:
filtering the *dim version* returns every fact, because of the onboarding sentinel below;
filtering the *fact* by `order_date` is what restricts business activity.)

### Module 11 — SCD-2 mechanics: close-then-open, sentinel, and the surrogate
**Concept.** Each dimension row carries `valid_from` / `valid_to` (`9999-12-31` = open) /
`is_current` / `row_hash`. On change: close the current row (set `valid_to` = change date,
`is_current = false`), open a new one. Onboarding backdates v1 to the **sentinel
`1900-01-01`** so *every* row has a version covering any as-of date — there is no such thing
as "a customer with no row valid on date X."
- `signup_date` (a business attribute) and `valid_from` (warehouse validity, sentineled to
  1900) live in the same row and mean totally different things. Don't mix them.
- A flat dimension (all `valid_to = 9999-12-31`, all `is_current = true`) just means no
  tracked attribute has changed yet — the machinery is built, not yet exercised.

**Check.** What would a customer who changed segment look like — how many rows, and what are
their `valid_from`/`valid_to`/`is_current`?

### Module 12 — The reproducible surrogate (the keystone)
**Concept.** `customer_key = xxhash64(customer_id, valid_from)` — a deterministic 64-bit hash
of natural key + version start. Three properties fall out for free:
- **Version-aware** — different `valid_from` per version → different key, automatically.
- **Deterministic** — re-run yields identical keys → idempotent `MERGE`, no drift.
- **Stateless / parallel** — no global counter, so every Spark partition computes it
  independently.

This is the deliberate break from Teradata's IDENTITY/SEQUENCE. A sequence needs a central
coordinator (a serialization point) and, worse, renumbers on reload — which would orphan
every fact. On distributed, stateless compute you trade a human-readable small int for an
opaque, replayable hash. (`xxhash64` is a Spark built-in; type-sensitive, so keep
`valid_from` the same type across all loads.)

**The fact never re-hashes.** `gold_fact` resolves the surrogate by a point-in-time join —
join source `customer_id` + `order_date` to the dim on
`order_date >= valid_from AND order_date < valid_to`, take that version's `customer_key`, then
**drop `customer_id`**. That's why `fact_sales` has `customer_key` but no `customer_id`: the
natural key is consumed and replaced by the as-of-order surrogate. Serving-layer joins then
use a plain equi-join on the surrogate and get as-of correctness for free.

**Check.** Why does the surrogate let you rebuild `dim_customer` without touching the 36k-row
`fact_sales`? (Next module — there's a sharp edge.)

### Module 13 — Decoupling, and its precondition (the sharp edge)
**Concept.** Because `customer_key = hash(customer_id, valid_from)`, the fact survives a dim
rebuild **if and only if the rebuild reproduces the exact same `valid_from` for every
version.** Natural key is stable; `valid_from` is the only variable.

| Rebuild from… | `valid_from` reproduces? | Fact stays valid? |
|---|---|---|
| Idempotent **re-run** of the same dated loads, same order | yes | ✅ |
| **Replay** of full dated history (the bronze append-log) | yes | ✅ |
| Only the **current OLTP snapshot** | **no** | ❌ |

The third row is the trap. `valid_from` for a split is *the date the ETL first observed the
change* — that information lives only in the dim and in **bronze**, never in the live OLTP
(which overwrites in place). Rebuild from current OLTP and every customer collapses to a
single 1900 sentinel version: flat customers keep their key, but any multi-version customer
gets a *different* `valid_from` → a *different* key → **orphaned facts**.

So the property isn't the hash alone — it's that **bronze is an immutable, dated append-log
you can replay.** The hash is the mechanism; retained dated history is the precondition. This
is the real reason the medallion insists bronze be append-only and never updated in place.

**Rule.** Rebuild a dimension by **replaying bronze in date order**, never by reading the live
source.

---

## Closing reflection — why this is beautiful

Thirty years from RDBMS OLAP to S3 / Parquet / Iceberg / Spark / Dremio, and we land back on
the same OLAP concepts — star schema, SCD-2, the same SQL. But it's a **helix, not a circle**:
it returns to the same angular position at a higher level each turn. You stand where you stood
years ago, looking at the same star schema, but you climbed to get back here.

The logical layer is invariant because it's **mathematics** — set theory and relational
algebra, Codd's work. Math doesn't churn. What churns is everything *below* the math: how
bytes are stored, who coordinates the transaction, where the CPU lives. Teradata fused all of
that into one vertically-integrated box you couldn't pry apart. The actual revolution isn't a
new idea — it's the **decomposition of the monolith into open contracts**:

- **Parquet** — a public file-format spec, not a vendor's internal page layout
- **Iceberg** — a public table-format contract (snapshots, schema evolution)
- **Arrow** — a public in-memory contract, so engines hand data over without re-serializing
- **a catalog (Nessie)** — the metadata pointer, also a contract

Because each seam is a *published contract* rather than a proprietary internal, every layer
became independently swappable, and able to live on the other side of the earth. The same
Parquet file on MinIO is read right now by Spark (writer) and Dremio (server), and could be
DuckDB or Trino tomorrow — none of them owns it. Teradata could never let four engines read
its storage; the storage *was* Teradata.

So we didn't "come back." The logical model never left — it was always the stable thing. What
changed underneath is **economic and structural**: compute that scales to zero, layers
oceans apart, engines picked per-workload, all reading one open copy of the truth. The
continuity at the top is what *lets* the violence of change happen safely below it.

Stable contract on top, swappable everything beneath — that is the entire art of it. And it's
the **same principle as `customer_key`**: a stable contract (the surrogate) that lets you
rebuild everything underneath without breaking what sits on top. The lakehouse is that idea
applied all the way down.

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
- [x] Module 10 — Two orthogonal "as of" axes
- [x] Module 11 — SCD-2 mechanics
- [x] Module 12 — The reproducible surrogate
- [x] Module 13 — Decoupling and its precondition

## Recommended pace

Part A in one sitting (it makes the abstract concrete fast). Part B is the heart — one
module per sitting, lots of looking at snapshots and files. Part C ties it back to the
DataHub track you already know. Tell me a module number and we start.
