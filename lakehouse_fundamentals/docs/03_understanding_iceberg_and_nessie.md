# Understanding Iceberg and Nessie

Layer 3 of the lakehouse — the **table layer**. S3 (Layer 1) stores opaque bytes; Parquet
(Layer 2) gives those bytes columnar structure. But a pile of Parquet files is not a *table* —
it has no name, no schema-of-record, no history, no ACID, no notion of "which files are current."
Layer 3 supplies all of that, and it does it in **two cooperating pieces**:

> **Iceberg is the table *format*** — a spec + metadata files that turn a set of immutable
> Parquet files into a table with a schema, snapshots, partitioning, and ACID semantics.
> **A catalog (here, Nessie) is the *pointer-keeper*** — the authoritative registry that answers
> "for table X, what is the current Iceberg metadata file *right now*." Iceberg defines the
> table; the catalog says which version of it is live.

Almost every "magic" lakehouse feature — schema evolution without rewrites, time-travel,
partition evolution, multi-engine sharing — is a clever manipulation of *metadata* over Parquet
files that **never change**. Hold onto that: **physical files are immutable and dumb; the
metadata layer is where identity, history, and layout live.**

---

## 1. The two halves of Layer 3 — table format vs catalog

These get conflated constantly. Keep them separate:

| | **Table format (Iceberg)** | **Catalog (Nessie / Hive / Glue / REST)** |
|---|---|---|
| Job | Define *what a table is* — schema, snapshots, partition spec, file list | Track *which metadata file is current* for each named table |
| Lives where | `metadata.json` + manifests + Parquet, all in object storage (MinIO) | A small service/DB holding a name → pointer mapping (+ history) |
| Swappable? | The format you standardize on (your firm: Iceberg) | Yes — Hive, Glue, REST, Nessie all catalog the *same* Iceberg tables |
| Analogy | The table's DDL + data + change log, serialized to files | The system catalog / data dictionary that knows table names |

**The key consequence (this bit us in the lab):** the same MinIO bucket can hold the files of
tables tracked by *different* catalogs. A folder existing in storage tells you **nothing** about
whether it's a live table, which catalog owns it, or which snapshot is current. Only the catalog
knows. **Storage shows bytes; the catalog defines tables.**

> **Teradata bridge.** In Teradata these two are fused and invisible — the DBMS *is* the storage,
> the optimizer, and the data dictionary all at once. The lakehouse pulls them apart into
> separate, independently swappable layers. Iceberg ≈ the on-disk table structure + transient
> journal; the catalog ≈ `DBC`/the data dictionary that resolves table names to objects.

---

## 2. Iceberg: what turns files into a table — the pointer chain

An Iceberg table is a **tree of metadata** sitting on top of immutable data files. A read walks
the chain top-down; a write appends a new top and re-points.

```
   catalog (Nessie)                ← "table m6.orders → THIS metadata file"
        │
        ▼
   vN.metadata.json                ← current schema, partition spec, snapshot list, current snapshot
        │
        ▼
   snap-####.avro  (manifest list) ← the set of manifests that make up this snapshot
        │
        ▼
   ####-m0.avro    (manifest)      ← list of data files (+ per-file partition values, row counts,
        │                            column min/max bounds)  ← used for pruning
        ▼
   data/.../*.parquet              ← the actual rows (immutable; Layer 2)
```

Two things to internalize:

- **Every write produces a new `metadata.json` (a new snapshot) and re-points the catalog.** The
  old files are never edited. An `INSERT` writes new data files + a new manifest + a new
  metadata.json, then flips the catalog pointer — *atomically*. That single pointer-flip is what
  makes Iceberg writes ACID: a reader sees either the old snapshot or the new one, never a
  half-written state.
- **The manifests carry stats** (partition values, row counts, column min/max). This is how
  Iceberg prunes *whole files* before opening them — the table-level equivalent of Parquet's
  footer stats.

**Why "a table is many files + metadata," not "a table is a file":** day 1 you insert 2 rows
(1 data file); day 2 you add 1 row → a *second* data file (the first is never touched). The table
is now the *set* {file1, file2} = 3 rows — recorded in a new snapshot. No single file holds all 3.
The metadata is the only thing that knows the table is "3 rows across 2 files."

---

## 3. Schema evolution — metadata-only, via field-IDs

Iceberg tracks every column by a stable **field-ID**, not by name or position. The schema is a
mapping `field-ID → (name, type)`. Consequences, all **metadata-only (no data rewrite)**:

- **ADD column** → new field-ID. Old data files don't carry it → reads return `NULL` for old rows.
- **RENAME** → same field-ID, new name label. Old files still read correctly (matched by ID).
- **DROP / REORDER** → just edit the ID→name map. Files untouched.

Because it's pure metadata, schema change is **O(1) regardless of table size** — instant on a
2-row table or a 2-billion-row table, no lock, no rewrite.

> **Teradata bridge.** `ALTER TABLE ADD COLUMN` is metadata-only in Teradata too — but RENAME,
> DROP, and REORDER being *equally free and safe* is the field-ID payoff. The physical files are
> immutable; the schema is just a field-ID *view* layered over them.

---

## 4. Snapshots & time-travel — immutability buys history for free

Every mutation (insert/delete/update/compaction) creates a new **snapshot** — an immutable
pointer to a file set, with a timestamp and a parent. The chain of snapshots *is* the table's
history. Because old files are never deleted on write, you can query the table **as of** any past
snapshot or timestamp:

```sql
SELECT * FROM t VERSION AS OF <snapshot-id>;
SELECT * FROM t TIMESTAMP AS OF '2026-06-19 09:00:00';
SELECT * FROM t.snapshots;   -- the history itself, as a queryable metadata table
```

Old snapshots (and their now-orphaned files) are reclaimed only when you explicitly run
`expire_snapshots` — history is retained until you say otherwise.

> **Teradata bridge.** This is journaling/temporal tables, but *free and built-in* — you don't
> design it; immutability gives it to you. The cost is storage (old files linger) until expiry.

---

## 5. Hidden partitioning & three-level pruning

**Hidden partitioning:** you partition by a *transform on a real column* — `months(order_date)`,
`days(ts)`, `bucket(16, customer_id)`, `truncate(10, zip)` — not by adding a separate partition
column. Iceberg computes the partition value on write and prunes automatically when you filter on
the *real* column. No `dt=` column, no special filter syntax. (Hive's leak — "filter on the
partition column or full-scan" — is gone.)

**Three nested levels of skipping**, each from stats you've already met:

1. **Partition pruning** (manifest) — skip whole partitions whose value can't match.
2. **File pruning** (manifest min/max) — within surviving partitions, skip files by column bounds.
3. **Row-group skipping** (Parquet footer) — within surviving files, skip row groups.

**Partition evolution** — the Teradata-can't trick: you can `ALTER` the partition spec (e.g.
`months` → `days`) on a *live, populated* table with **zero data rewrite**. Old files keep their
old spec, new files use the new one, queries span both. (Changing a Teradata PPI requires an
empty table or a full reload.)

> **Partition ≠ PI trap (carries over verbatim):** partition on something too fine-grained (raw
> timestamp, high-cardinality id) and you manufacture a small-files disaster. The transforms
> (`months`/`days`/`bucket`) exist precisely to collapse cardinality to a sane partition count —
> same judgment as choosing PPI interval granularity.

---

## 6. ACID: how you mutate immutable files (COW vs MOR)

Files can't be edited, so a `DELETE`/`UPDATE`/`MERGE` resolves one of two ways — chosen per table:

- **Copy-on-write (COW)** — rewrite the affected file *minus* the deleted rows (or *with* the
  updated ones) into a new file; dereference the old. **Reads fast** (just read current files),
  **writes amplified** (rewrote rows you didn't change). Default; good for read-heavy/batch tables.
- **Merge-on-read (MOR)** — leave the data file untouched; write a small **delete file** ("rows at
  positions X in file Y are gone"). **Writes cheap**, **reads slower** (must merge data + delete
  files every scan). Good for write-heavy/streaming/CDC.

`MERGE INTO` (upsert) is your **SCD2** in lakehouse form — match-update-or-insert atomically; under
MOR an update decomposes into delete-old + insert-new. The accumulated MOR debt is swept by
**compaction** (`rewrite_data_files` for data, `rewrite_position_delete_files` for deletes) plus
`expire_snapshots` for GC — the explicit, schedulable equivalent of a Teradata **REORG/PACK**.

Edge case worth knowing: when *all* rows of a file are deleted, both COW and MOR just drop the
file from the manifest (no delete file). Delete files only appear for **partial** deletes.

---

## 7. The catalog — why it exists at all

Given §2's pointer chain, something must hold the very top pointer: **"table `m6.orders` → this
exact `metadata.json` right now."** That's the catalog's whole job. Without it you'd be guessing
which `metadata.json` is current by scanning the bucket — unreliable, racy, and unsafe under
concurrent writes.

A catalog also provides:
- **Atomic commit** — flipping the pointer is the atomic step that makes a write all-or-nothing.
- **A namespace** — human table names (`sales.transactions`) instead of S3 paths.
- **Concurrency control** — optimistic locking on the pointer so two writers don't clobber.

Catalog implementations (all cataloging the **same** Iceberg tables): filesystem/Hadoop, Hive
Metastore, AWS Glue, generic Iceberg REST, **Nessie**. Switching catalogs later is portable —
re-register the pointers (`register_table` / catalog-migrator); the data files don't move.

---

## 8. Nessie — the versioned catalog (git for data)

Nessie does the catalog job above, then adds one big idea: **the catalog itself is versioned,
git-style.**

- **References** — `branches` and `tags`, exactly like git.
- **Commits** — every catalog change (create table, insert, schema change) is an immutable commit
  with a hash, author, timestamp, parent. One *global* timeline across **all** tables on a branch.
- **Pointers, not data** — Nessie stores, per table, only the metadata-file location (+ the commit
  history of how that pointer moved). The Iceberg metadata and Parquet still live in MinIO. Nessie
  needs **no** S3 access; the engines hold the storage credentials.

What the git model unlocks beyond a plain catalog:
- **Multi-table atomic commits** — change 5 tables in one commit; readers see all-or-nothing across
  tables (a plain catalog can only do per-table atomicity).
- **Whole-catalog time-travel** — "what did the *entire* catalog look like at commit `abc123`?"
- **Zero-copy branching** — branch the entire multi-table catalog instantly (just pointers, no data
  copied). Write/validate on an `experiment` branch; `main` (what other engines serve) is
  unaffected until you merge. An instant, zero-copy clone of the whole "database" for dev/test.

> **Teradata bridge.** Imagine branching your *entire* data dictionary + all tables in O(1), with
> no storage cost, then merging or discarding the branch. There's no Teradata equivalent — it
> falls out of "everything is an immutable pointer."

Nessie has a built-in **web UI** (catalog/version browser: references, commit log, table tree) — it
is *not* a data browser; you can't `SELECT` rows there. (Iceberg, the format, has no UI at all —
a spec has no dashboard; you inspect it via metadata tables through an engine, or the catalog UI.)

---

## 9. Why Nessie in *this* lab

Two engines must share one catalog so Spark (writer) and Dremio (reader) see the same tables at
the same snapshots — Jianmin's hard requirement: *access at the Iceberg layer, never raw S3.*

- Dremio OSS's **generic Iceberg REST Catalog** source is **Enterprise-only** (>25.2.0).
- Dremio OSS's **Nessie** source **is** in OSS.

So Nessie is the OSS-viable shared catalog. Tables stay 100% Iceberg; only the catalog changed.
Lab topology that resulted:

```
   Spark (Zeenie) ──writes──┐                    ┌──reads── Dremio (Zeenie :9047)
                            ▼                    ▲
                      Nessie catalog  (:19120, RocksDB, branch=main)
                            │  pointer → s3://warehouse/.../metadata.json
                            ▼
                 Iceberg metadata + Parquet  in MinIO (:9000 / console :9001)
```

Spark connects via Iceberg's `NessieCatalog` (jar `iceberg-nessie`, baked into `spark-defaults`);
Dremio via its native Nessie source. They never talk to each other — they meet at the catalog.
DuckDB (Lenovo) is a third potential reader of the same files. **Same files, swappable engines —
the decoupled-layer architecture, proven both directions.**

---

## 10. The full chain, end to end

```
ENGINE        Spark / Dremio / DuckDB        "give me table sales.transactions"
   │
CATALOG       Nessie (:19120)                resolves name → current metadata.json + version
   │
TABLE FORMAT  Iceberg metadata               snapshot → manifest list → manifests
   │          (in MinIO)                      (prune partitions & files via stats)
   │
FILE FORMAT   Parquet                         columnar data; footer stats prune row groups
   │          (in MinIO)
STORAGE       S3 / MinIO (:9000)              fetch the surviving byte ranges
```

A query enters at the engine, asks the catalog for the pointer, walks the Iceberg metadata
(pruning as it goes), and reads only the surviving Parquet byte ranges from MinIO. **No engine
talks to another engine; they all talk to Iceberg-via-the-catalog.**

---

## 11. Hard-won lessons (from building this)

- **Catalog ≠ storage.** MinIO listed 4 folders; Nessie listed 2. The extras were files of tables
  registered in a *different* (retired) catalog — present in storage, invisible to Nessie. A bucket
  folder is not a table.
- **Always read at the Iceberg layer (via the catalog), never raw S3.** A raw-Parquet reader would
  resurrect MOR-deleted rows, double-count compacted files, misread evolved schemas, and not know
  the current snapshot. The catalog + Iceberg metadata is the *only* coherent view.
- **Drop the catalog pointer *before* wiping storage.** Wipe first and `DROP TABLE` 404s trying to
  load deleted metadata; you then have to delete the pointer at the catalog layer directly (Nessie
  commit with a `DELETE` op). Catalog and storage are independently mutable — in both directions.
- **In-memory catalog = data loss on restart.** Nessie defaults to `IN_MEMORY`; a restart wipes the
  catalog (data files orphaned in MinIO). For durability use an on-disk store (we use **RocksDB** on
  a named volume) so the catalog survives reboots.
- **Dangling deletes are real.** After compaction, a position-delete file can linger pointing at a
  vaporized data file — harmless (applies to nothing) but metadata cruft; cleared by
  `expire_snapshots` / `rewrite_position_delete_files`. Production tables need a scheduled
  maintenance job (compaction + expiry) — the lakehouse REORG cadence.

---

## 12. Teradata cheat-sheet

| Teradata concept | Iceberg / Nessie equivalent |
|---|---|
| Data dictionary (`DBC`) resolving table names | The **catalog** (Nessie) resolving name → metadata pointer |
| `COLLECT STATISTICS` (separate, goes stale) | Manifest + Parquet stats — written *with* the data, never stale |
| Partition elimination (PPI) | Partition pruning via `months()`/`days()`/`bucket()` transforms |
| Changing a PPI (empty table / full reload) | **Partition evolution** — metadata-only, live table, no rewrite |
| `ALTER TABLE ADD COLUMN` (metadata-only) | Schema evolution by field-ID — add/rename/drop/reorder all free |
| REORG / PACK / space reclamation | `rewrite_data_files` + `rewrite_position_delete_files` + `expire_snapshots` |
| Journaling / temporal tables | Snapshots + time-travel (free, from immutability) |
| `MERGE` for SCD2 | `MERGE INTO` (COW or MOR); MOR = delete-old + insert-new |
| (no equivalent) | **Nessie branches/tags** — zero-copy clone of the whole catalog |

---

*Lab specifics (versions/ports/config) are intentionally light here — see the compose and
`spark-defaults.conf` under `lakehouse_fundamentals/docker/zeenie/` for the running setup. This doc
captures the concepts; the repo captures the wiring.*
