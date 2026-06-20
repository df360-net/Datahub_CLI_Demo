# Understanding Parquet

Layer 2 of the lakehouse — the **file format**. S3 (Layer 1) stores opaque blobs of bytes;
Parquet is what those blobs *are*. The single most important idea:

> **Parquet is a columnar, self-describing file. It stores data column-by-column (not
> row-by-row), carries its own schema and per-column statistics inside the file, and is laid
> out so a reader can skip most of it — reading only the columns and the row ranges a query
> actually needs.**

Everything fast about lakehouse analytics — column pruning, predicate pushdown, cheap
compression — falls out of that one layout choice. This is the columnar "why" behind the
whole stack (and the reason a row store like MSSQL is slow at big scans).

---

## 1. What Parquet is — and isn't

A Parquet file is **one immutable object** (one S3 key, written once, never edited in place —
which is exactly why it pairs with object storage and Iceberg). Inside, it is **not** a flat
dump of rows. It is a structured container with three defining traits:

- **Columnar** — values of one column are stored *contiguously*, not interleaved with the rest
  of the row. All the `amount` values sit together; all the `label` values sit together.
- **Self-describing** — the file carries its **own schema** (column names, types, nesting) and
  its own **statistics** (min/max/null-count per column per chunk) in a footer. You never need
  an external DDL to read it. Hand someone the file and they can read it cold.
- **Built to be skipped** — the layout + footer let a reader open the file, read a small
  footer, and then fetch *only* the byte ranges it needs. Reading is mostly *not reading*.

What it is **not**:
- **Not row-oriented** — the opposite of MSSQL/Teradata's row storage, where a whole row sits
  together. (More precisely: Parquet is a *hybrid* — rows are first batched into row groups,
  then stored columnar *within* each group. See §2.)
- **Not a table** — it has no name, no catalog, no snapshots, no ACID. That's Iceberg's job
  (Layer 3). A Parquet file is just a self-describing chunk of columnar data. A table is
  *many* Parquet files plus Iceberg metadata tracking them.
- **Not mutable** — no in-place UPDATE, no append-into-the-middle. A change means writing a
  **new** file. (Unlearn the in-place-UPDATE instinct here; it's the file format enforcing it.)
- **Not human-readable** — it's a binary format. You inspect it with tools (`parquet-tools`,
  PyArrow), not `cat`. Contrast Iceberg's `metadata.json`, which *is* readable JSON.

### Row store vs columnar — the core picture

The same four-row table, two ways to lay the bytes on disk:

```
Logical table:
  id | amount | label
   1 |  10.0  |  "a"
   2 |  20.0  |  "b"
   3 |  30.0  |  "c"

Row store (MSSQL, Teradata):   [1,10.0,"a"][2,20.0,"b"][3,30.0,"c"]
                                 one row, whole, then the next row

Columnar (Parquet):            [1,2,3]      [10.0,20.0,30.0]      ["a","b","c"]
                                all ids      all amounts           all labels
```

Why the columnar layout wins for analytics — three consequences, all from "same column =
contiguous bytes":

1. **Column pruning (read less).** `SELECT sum(amount)` touches *only* the `amount` bytes;
   `id` and `label` are never read off disk. A row store must read every full row and throw
   away the columns it doesn't need. On a 200-column table reading 3 columns, that's ~98% less
   I/O.
2. **Better compression.** A column is **homogeneous** — all the same type, often similar or
   repeating values. Compressors and encodings (§4) crush homogeneous data far harder than the
   mixed-type jumble of a row. Columnar files are typically several times smaller.
3. **Vectorized execution.** A contiguous run of same-type values is exactly what a CPU wants —
   tight loops, SIMD, no per-row type dispatch. Engines process a column as a batch.

The flip side — and the rule for *when columnar hurts*: an OLTP "fetch one whole order by id"
reassembles a full row from N scattered column locations. A row store grabs that row in one
contiguous read; columnar has to gather N pieces. **Columnar is for scans over few columns and
many rows; row stores are for point-lookups of whole rows.** This is the OLAP/OLTP divide made
physical — and why your warehouse and your OLTP system want different storage.

**Teradata bridge:** if you've used Teradata's **columnar partitioning** (column-partitioned
tables / "Teradata Columnar"), Parquet is that idea as the *default, file-level* layout — every
file is column-partitioned, always. The instinct you built for "narrow projection scans get
cheaper" transfers directly.

---

## 2. The physical structure: File → Row Group → Column Chunk → Page

Parquet is **hybrid columnar**, not pure columnar. It first cuts the rows into batches
(**row groups**), then stores columnar *within* each batch. Four nested levels, plus the
footer:

```
Parquet file
├── Row Group 0          ← a HORIZONTAL slice: a batch of rows (e.g. ~1M rows / ~128 MB)
│   ├── Column Chunk: id        ← a VERTICAL slice: all id values for THIS row group
│   │   ├── Page 0   (a few thousand values + encoding + page stats)
│   │   ├── Page 1
│   │   └── ...
│   ├── Column Chunk: amount
│   ├── Column Chunk: label
│   └── ...
├── Row Group 1
│   └── (same columns, next batch of rows)
├── ...
└── Footer  ← schema + per-row-group, per-column STATS + byte offsets (the map). At the END.
```

### The two axes — the mental model that matters

A Parquet file is cut on **two independent axes**, and keeping them straight is the whole game:

| Cut | Axis | Structure | Splits by |
|---|---|---|---|
| **Rows** | **horizontal** | **Row Group** | a batch of rows (size-based, ~128 MB) |
| **Columns** | **vertical** | **Column Chunk** | one column, within one row group |

- A **row group** is a **horizontal** slice — a batch of *rows*. (Common trip-up: "row-wise
  split" sounds vertical, but splitting *by rows* is **horizontal** — picture a horizontal
  knife cut separating groups of rows.)
- A **column chunk** is a **vertical** slice — one *column's* data, *inside* one row group.
- So `Row Group 3, column amount` = one **column chunk**: the `amount` values for that batch
  of rows, stored contiguously and compressed on their own.
- A **page** is the smallest unit *inside* a column chunk — a few thousand values that get
  encoded and compressed together. It's the atom of read/decompress.

**Do not confuse a row group with a table partition.** Both are "horizontal," but they live at
different layers and do different jobs:

| | **Parquet row group** | **Iceberg / table partition** |
|---|---|---|
| Layer | 2 (the file) | 3 (the table format) |
| Split by | **size** (~128 MB batch of rows) | **column value** (e.g. `days(ts)`) |
| Scope | *within one file* | *across separate files* |
| Query-visible? | no — physical/internal | yes — you `PARTITIONED BY` a column |
| Purpose | I/O unit + stats-skipping granularity | file pruning |

A row group is a **physical, size-based batch inside one file** (Parquet's business). A
partition is a **logical, value-based split into separate files** (Iceberg's business, Module
6). Your Teradata **PPI** instinct maps to the *partition*, not the row group.

### Why the footer is at the END

A writer streams rows out and only knows the final byte offsets and complete statistics *after*
it has written all the data. So the **footer is written last, and lives at the tail** of the
file (ending with a 4-byte length + the magic number `PAR1`). The reader's dance:

1. Seek to the end, read the last 8 bytes → get the footer length + `PAR1` magic.
2. Read the footer → now it has the **schema, every row group's stats, and the byte offset of
   every column chunk**. The footer is the **map of the file**.
3. Use the map to fetch *only* the needed column chunks, from the needed row groups, by **byte
   range** (an S3 range-`GET`).

This is why Parquet on S3 is efficient: a read is *footer, then a few targeted range reads* —
never "download the whole file and scan."

**Teradata bridge:** the footer is **COLLECT STATISTICS baked into the file**. Where Teradata
keeps stats in the data dictionary and the optimizer consults them separately, Parquet carries
min/max/null-counts *with the data*, per row group. The optimizer's "can I skip this?" check
reads the footer instead of a stats table. The row group is your **data block / I/O unit** —
the granularity at which the engine decides "read or skip."

---

## 3. Encodings — making columns small *before* compression

A column chunk isn't stored as raw values. Parquet first **encodes** the values using
type-aware schemes that exploit the homogeneity of a column. Encoding is lossless and
structural — it's *not* the same as compression (§5), and it usually does most of the
size reduction. The main schemes:

- **Dictionary encoding** (the workhorse). Build a dictionary of distinct values; store each
  value as a small integer index into it. A `country` column of 200M rows with 195 distinct
  values becomes 195 strings + 200M tiny ints. Devastatingly effective on **low-cardinality**
  columns. Falls back to plain encoding if the dictionary grows too large.
- **Run-length encoding (RLE).** Store "value V, repeated N times" instead of N copies. Crushes
  **sorted or clustered** columns and long runs of the same value (or of nulls). This is *why
  sort order matters for file size*, a knob you own (§8).
- **Bit-packing.** If a column's values fit in 3 bits, don't waste 32. Packs small integers
  (and dictionary indices) tightly. Usually paired with RLE (`RLE/BIT_PACKED`).
- **Delta encoding.** Store differences between consecutive values, not the values. Great for
  **sorted ids, timestamps, sequence numbers** — a monotonically increasing `event_id` becomes
  a base + a stream of small deltas.

The encoding is recorded **per page**, so different pages of the same column can use different
schemes, and a reader knows how to decode each from the page header.

**Teradata bridge:** dictionary + RLE are the same family as Teradata's value-list compression
and the dictionary compression of columnar tables — you already know "low-cardinality columns
compress nearly for free." The lakehouse twist: the encoding is chosen automatically per page,
and **sort order is your lever** to turn good encoding into great encoding.

---

## 4. Compression — the second squeeze

*After* encoding, each page is optionally run through a general-purpose **compression codec**.
Two distinct stages, easy to conflate:

```
raw values → [ENCODING: dictionary/RLE/delta/bit-pack] → [COMPRESSION: snappy/zstd/gzip] → bytes on disk
              structural, type-aware, lossless            generic byte compressor
```

Codec choices and the tradeoff you're picking:

| Codec | Ratio | CPU | When |
|---|---|---|---|
| **Snappy** | modest | very low | default — fast read/write, "good enough" (the common lakehouse default) |
| **zstd** | high | moderate (tunable level) | cold/archival data, or when storage/scan-bytes dominate — increasingly the smart default |
| **gzip** | high | high | max ratio, rarely worth the CPU vs zstd |
| **none** | — | — | already-compressed payloads |

Compression is **per page / per column chunk**, so a reader decompresses only the pages it
actually reads — pruned columns and skipped row groups are never decompressed. The cost is
real CPU on the read path, which is why Snappy (cheap to decompress) is the default and zstd
is the "I'd trade some CPU for fewer bytes" choice.

**The decision is yours to make.** This is part of "you own more operational tuning than a
Teradata DBA did" — the appliance picked compression for you; here you choose the codec (and
zstd level) per table based on hot-vs-cold and scan-vs-store economics.

---

## 5. Statistics and predicate pushdown — *not reading* as a feature

This is where columnar layout + the footer pay off the most. Each column chunk (and each page)
carries **statistics** in the footer:

- **min** and **max** value
- **null count**
- (often) **distinct count**

The engine uses these for **two independent kinds of skipping** — and a great lakehouse user
designs to maximize both:

1. **Column pruning (skip columns).** From the query's projection: `SELECT sum(amount)` reads
   only the `amount` column chunks; every other column's bytes are never fetched. Vertical
   skip.
2. **Predicate pushdown / row-group skipping (skip rows).** From the query's `WHERE`: for
   `WHERE amount > 1000`, the engine reads each row group's `amount` **max** from the footer;
   if `max <= 1000`, the *entire row group is skipped* without reading its data. Horizontal
   skip. (Same logic at the finer page level.)

```
SELECT sum(amount) WHERE amount > 1000

footer says:
  Row Group 0:  amount min=5    max=80     → max <= 1000 → SKIP ENTIRELY (no read)
  Row Group 1:  amount min=200  max=5000   → might match → read amount chunk, filter
  Row Group 2:  amount min=10   max=400    → max <= 1000 → SKIP ENTIRELY

→ read only Row Group 1's amount column. Possibly <1% of the file touched.
```

**The catch that decides whether this works: stats only help if the data is *clustered* on the
filter column.** If `amount` is randomly scattered, *every* row group's `[min,max]` spans the
whole range, every range overlaps every predicate, and **nothing gets skipped** — you read the
whole column. If the data is **sorted/clustered** on `amount`, each row group covers a tight,
non-overlapping range and the engine skips almost everything. This is the single biggest
file-layout lever you own, and it's the lakehouse version of a principle you know cold:

**Teradata bridge:** min/max row-group skipping **is partition elimination / PPI pruning**,
done at the row-group level inside a file by statistics instead of by partition boundaries.
"Sort the data so the optimizer can eliminate I/O" is exactly the instinct you built tuning
PPI and join indexes — it transfers directly. The difference: there are **no secondary
indexes** here. Your only knobs to make pruning work are **sort order, clustering, and
partitioning** (write-time file layout) — not indexes you add after the fact.

---

## 6. The read path, end to end

Putting it together — what an engine actually does to run `SELECT sum(amount) WHERE amount >
1000` against a Parquet file on S3:

1. **Range-GET the footer** (last few KB) from S3. One small request.
2. **Parse the footer:** schema, row-group list, per-row-group column stats, byte offsets.
3. **Column pruning:** from the projection + predicate, decide it needs only the `amount`
   column. Ignore all other column chunks' offsets.
4. **Row-group skipping:** check each row group's `amount` `[min,max]`; drop the ones that
   can't match.
5. **Targeted range-GETs:** issue an S3 range read for *just* the surviving `amount` column
   chunks. (Many in parallel — this is where S3's throughput-via-fan-out from
   [01_understanding_S3.md](01_understanding_S3.md) §6 shines.)
6. **Decompress + decode** only those pages (Snappy/zstd → dictionary/RLE/delta → values).
7. **Apply the filter and aggregate**, vectorized over the contiguous column batch.

Note how much *didn't* happen: other columns never left S3, skipped row groups never left S3,
their pages were never decompressed. **The fastest read is the one you avoid** — and the whole
format exists to let the engine avoid as much as possible.

---

## 7. The knobs you own (the advanced-user surface)

Parquet is mostly automatic, but a lakehouse practitioner deliberately controls a handful of
write-time settings. These are the difference between a table that prunes beautifully and one
that scans fully:

| Knob | Effect | Guidance |
|---|---|---|
| **Row group size** (~128 MB default) | granularity of skipping + the I/O unit | bigger = better compression, fewer requests, coarser skipping; smaller = finer skipping, more overhead. Tune to query selectivity. |
| **Page size** (~1 MB default) | finest decode/skip granularity | rarely touched; smaller helps very selective point-ish reads. |
| **Compression codec** | size vs read CPU | Snappy default; **zstd** for cold/scan-heavy. |
| **Sort / clustering order** | whether min/max pruning *works* | **the highest-leverage knob.** Sort on common filter columns so row-group `[min,max]` ranges stay tight and non-overlapping. |
| **File size** (Iceberg target, ~128–512 MB) | small-files problem | keep files large; that's what **compaction** (Module 8) fixes. |

**The small-files trap (your #1 thing to watch).** Frequent small writes → many tiny Parquet
files → a footer + per-file overhead + an S3 round-trip *per file*. Death by a thousand
requests, the same pathology from [01_understanding_S3.md](01_understanding_S3.md) §6. The fix
is **compaction**, and avoiding it starts at write time (batch writes, sensible row-group
sizing). Coming from Teradata, this is a *new* failure mode — the appliance hid file layout
from you; here it's yours to manage.

---

## 8. Where Parquet sits in the lakehouse

Parquet is **Layer 2**, and it's deliberately dumb about tables — which is what lets the layers
above it stay swappable:

```
Engine        (Spark/Trino/DuckDB)   reads/writes, does the skipping decisions
   ▲
Table format  (Iceberg)              tracks WHICH Parquet files are the table, snapshots, schema-by-field-id
   ▲
File format   (PARQUET)  ← you are here   one immutable, self-describing, columnar file
   ▲
Object store  (S3/MinIO)             stores each Parquet file as one opaque object/key
```

Two relationships worth pinning down:

- **Parquet ⟷ S3 (below):** each Parquet file is **one S3 object** — one key, written once,
  immutable. Parquet's no-in-place-edit nature and S3's whole-object-replace nature are the
  same constraint, which is why they fit (see [01_understanding_S3.md](01_understanding_S3.md)
  §1).
- **Parquet ⟷ Iceberg (above):** Iceberg's manifests **copy the per-file Parquet stats**
  (min/max/null counts, row counts) up into the manifest layer. So Iceberg can prune *whole
  files* by reading manifests — *before* it ever opens a Parquet footer. It's the **same
  min/max idea at two levels**: Iceberg skips files via manifests; Parquet skips row groups via
  its footer. Stats are duplicated up a level precisely so the engine can prune as early and as
  cheaply as possible. (This is also why Iceberg never has to *list* an S3 directory — see
  [01_understanding_S3.md](01_understanding_S3.md) §5.)

So the same query gets *two* rounds of skipping: Iceberg drops files it can't need, then
Parquet drops row groups within the surviving files. Both rounds are powered by sorting your
data so min/max stays tight.

---

## 9. Hands-on: crack open a real Parquet file

We built `demo.smoke.wide` in Module 2 (`id, a, b, label` over a million rows). Let's look at
its actual physical structure. From Jupyter (or `docker exec -it spark-iceberg spark-sql`):

**a. See the data files Iceberg is tracking, with row counts and sizes:**
```python
spark.sql("SELECT file_path, record_count, file_size_in_bytes "
          "FROM demo.smoke.wide.files").show(truncate=False)
```

**b. Inspect the Parquet internals with PyArrow** (in a Python notebook cell). Grab one file
path from (a), then:
```python
import pyarrow.parquet as pq
# read the file straight off MinIO via the S3 filesystem Spark already configured,
# or copy one out with mc and read locally. Then:
pf = pq.ParquetFile("/tmp/one_file.parquet")
print("row groups:", pf.num_row_groups)
print("schema:", pf.schema)
md = pf.metadata
rg = md.row_group(0)
print("rows in row group 0:", rg.num_rows)
for i in range(rg.num_columns):
    col = rg.column(i)
    print(col.path_in_schema,
          "| codec:", col.compression,
          "| encodings:", col.encodings,
          "| min:", col.statistics.min,
          "| max:", col.statistics.max,
          "| nulls:", col.statistics.null_count,
          "| size:", col.total_compressed_size)
```

**c. Pull one file out to inspect with `mc`** (the storage-layer view):
```
docker exec -it mc sh
mc alias set m http://minio:9000 admin password
mc ls -r m/warehouse/smoke/wide/data/        # the Parquet objects
mc stat m/warehouse/smoke/wide/data/<file>   # size / metadata of one object
```

What to look for, and the point each one proves:
- **`num_row_groups`** > 1 on a big file → the horizontal slicing of §2 is real.
- **`encodings`** showing `DICTIONARY` / `RLE` → §3 in action; `label` (a string) vs `id` (a
  sequence) likely differ.
- **`min`/`max` per column** → the §5 stats the optimizer skips with. Because `id` was written
  by `range(...)`, each row group's `id` `[min,max]` should be a tight, *non-overlapping* band
  — the best case for pruning. Confirm it, then imagine the same column shuffled randomly:
  every range would overlap and skipping would die. That contrast *is* the sort-order lesson.

---

## In one paragraph

Parquet is the columnar file format at Layer 2: data stored column-by-column, cut into row
groups (horizontal batches) of column chunks (vertical slices) of pages, with a footer at the
end that maps the file and carries per-column min/max/null statistics. That layout buys three
things a row store can't: read only the columns you need (column pruning), skip the row groups
that can't match your filter (predicate pushdown), and compress homogeneous columns hard
(encoding + codec). The stats only help if your data is **sorted/clustered** on the columns you
filter — making that true, by controlling sort order and file/row-group sizing, is the
highest-leverage thing you own. It's partition elimination and COLLECT STATISTICS you already
know from Teradata, rebuilt inside an immutable file on object storage — with no secondary
indexes to lean on, so file layout *is* the tuning.
