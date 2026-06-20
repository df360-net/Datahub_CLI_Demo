# Understanding Spark

Layer 4 of the lakehouse — the **processing engine**. Layers 1–3 are all *storage*: S3 holds
bytes, Parquet structures them, Iceberg+the catalog turn them into tables. None of them *compute*.
Spark is the engine that reads those tables, transforms them across a cluster of machines, and
writes new tables back. It is the lakehouse's equivalent of the part of Teradata you can't see —
the parsing engine, the AMPs, the BYNET, and the optimizer — except pulled out into a separate,
swappable layer that talks to the storage stack through Iceberg.

> **Spark is a distributed execution engine. You write a *declarative* query (DataFrame / SQL);
> Spark compiles it to a physical plan, splits the data into *partitions*, and runs the work as
> *tasks* across *executors* — moving data between machines only when it absolutely must (a
> *shuffle*). Performance is almost entirely about controlling partitions and minimizing /
> de-skewing shuffles.**

Everything in this doc reduces to one sentence: **the shuffle is the BYNET redistribution, and
your job is to avoid it, shrink it, or de-skew it.** If you internalize that, you understand Spark
performance.

> **Teradata bridge, up front.** Spark is a shared-nothing MPP engine — the same family as
> Teradata. The vocabulary maps almost 1:1, so lead with your instincts and just relearn the
> names. The cheat-sheet at the bottom is the whole Rosetta Stone.

---

## 1. Cluster anatomy — driver & executors

A Spark application has two kinds of process:

- **Driver** — the brain. Holds the `SparkSession`, builds the query plan, schedules work, tracks
  progress. One per application. *(≈ Teradata **Parsing Engine** + the dispatcher.)*
- **Executors** — the muscle. JVM processes that actually hold partitions in memory and run tasks
  on their cores. Many per application. *(≈ Teradata **AMPs**.)*

> **Teradata bridge.** Driver ≈ PE (parses, plans, dispatches); executors ≈ AMPs (own a slice of
> data, do the work in parallel); the network between them ≈ the **BYNET**. The difference: in
> Teradata the AMPs and their data assignment are fixed hardware/config; in Spark executors are
> requested at runtime and the data-to-executor assignment is recomputed every stage.

**Lab note:** our Spark runs `local[*]` — driver and "executor" are one JVM, and parallelism =
number of cores. On Zeenie that's **12** (`spark.sparkContext.defaultParallelism` → 12). A real
cluster spreads executors across machines, but the execution model below is identical.

---

## 2. Lazy vs eager — transformations build a plan, actions run it

Spark splits every operation into two classes:

- **Transformations** are **lazy** — `select`, `filter`, `withColumn`, `join`, `groupBy`. They
  build up a *plan* (a DAG of steps) and execute **nothing**.
- **Actions** are **eager** — `count`, `show`, `collect`, `write`. An action is what triggers
  Spark to actually optimize the accumulated plan and run it.

```python
df2 = df.filter(...).withColumn(...).join(...)   # nothing has run — just a plan
df2.count()                                      # ACTION → now Spark plans + executes
```

> **Teradata bridge.** Closer to how you'd *think* about a query than how Teradata behaves: in
> Teradata you submit one SQL statement and it runs. In Spark you chain many lazy steps, and the
> engine fuses and optimizes the *whole* chain only when an action demands a result — so it can
> push filters down, combine steps, and pick join strategies across the entire pipeline at once.

**Consequence for reading the UI:** each *action* becomes one or more **jobs**. If your notebook
ran 13 jobs, you triggered ~13 actions (or AQE re-planned a query into several jobs — see §10).

---

## 3. Jobs → stages → tasks (and where stages split)

The execution hierarchy:

```
action            →   one query
   job            →   one action's execution (AQE may split a query into several jobs)
      stage       →   a span of work with NO data movement inside it
         task     →   one stage's work on ONE partition (the atom of execution; runs on one core)
```

**The rule that matters: a new stage begins at every shuffle.** Within a stage, each partition's
work is independent and pipelined (a "narrow" dependency — `map`, `filter`). The moment an
operation needs data from *other* partitions (a "wide" dependency — `join`, `groupBy`, `repartition`),
Spark must **shuffle**, and that boundary cuts one stage from the next.

> **# stages = # shuffles + 1.** A query with one `groupBy` = 2 stages (map side, then reduce
> side). Two shuffles = 3 stages. Count the `Exchange` nodes in the plan and you've counted the
> stage boundaries.

**Tasks per stage = partitions in that stage.** 12 input partitions → 12 tasks in the read stage.
200 post-shuffle partitions → 200 tasks in the reduce stage. This is why partition count *is*
parallelism.

> **Teradata bridge.** A stage ≈ a step in the EXPLAIN plan that runs AMP-local; a stage boundary
> ≈ a BYNET redistribution step between two local steps. Tasks ≈ the per-AMP execution of a step.

---

## 4. The partition — the unit of parallelism, and where partitions come from

A **partition** is a chunk of rows processed by exactly **one task on one core**. Partition count
is the single most important performance number: too few → no parallelism + giant tasks that
spill; too many → scheduling overhead + tiny output files.

Partitions come from exactly **three** sources:

1. **At read time** — driven by input file count & size. Spark packs file splits into chunks of
   `spark.sql.files.maxPartitionBytes` (**default 128 MB**). One 1 GB file → ~8 partitions; 1000
   tiny files → up to 1000 partitions (the **small-files problem**). For a source like
   `spark.range(N)` with no files, it's `defaultParallelism` (= cores).
2. **After a shuffle** — `spark.sql.shuffle.partitions` (**default 200**). *Every* wide operation
   re-partitions the data into 200 partitions by default, regardless of data size (see §5).
3. **Explicitly** — `repartition(n [, col])` (full shuffle, even sizes, can hash by a key) vs
   `coalesce(n)` (no shuffle, merges adjacent partitions cheaply, can be uneven; only *reduces*
   count).

Inspect it any time by dropping to the underlying **RDD** (the low-level distributed-collection
abstraction the DataFrame compiles down to):

```python
df.rdd.getNumPartitions()                          # how many partitions right now
df.groupBy(F.spark_partition_id()).count()         # how many rows landed in each physical partition
```

> **Teradata bridge.** A partition ≈ the slice of a table an AMP owns. `getNumPartitions()` ≈
> "how many AMPs is this data spread across right now." The twist: in Teradata that's fixed by the
> PI at table-design time; in Spark it changes every stage and you control it per-operation.

---

## 5. The shuffle — the BYNET moment — and the `200` knob

A **shuffle** re-partitions data by the **hash of a key** (the join key, the groupBy key) so that
all rows sharing a key land in the same partition, ready to be joined/aggregated together. It costs
network + disk-spill + serialization — it is the most expensive thing Spark does. **This is the
BYNET redistribution.** In the physical plan it shows as an **`Exchange hashpartitioning(key, …)`**
node.

The infamous default: `spark.sql.shuffle.partitions = 200`. Every shuffle produces **200**
post-shuffle partitions, *data-blind* — it has no idea whether your result is 50 rows or 5 billion.

- **Too small for the data** → each partition too big → disk spill / OOM.
- **Too large for the data** → 200 tiny tasks (scheduling overhead) + 200 tiny output files.

We saw this directly: a `groupBy` producing **50 groups** still created **200** reduce tasks with
AQE off — `12 (map) + 200 (reduce) + 1 (final) = 213` tasks, ~150 of them doing nothing.

> **Teradata bridge.** `shuffle.partitions` ≈ the number of hash buckets a redistribution targets.
> Picking it by hand for every query was the classic pre-AQE Spark footgun — like being forced to
> choose hash-bucket granularity per query with no optimizer help.

---

## 6. Two-phase aggregation — local-aggregate-before-redistribute

Spark doesn't ship raw rows across the shuffle for a `groupBy`/aggregation. It does a **partial
aggregation on the map side first** (`partial_sum`, `partial_count` per partition), shuffles only
the small partial results, then a **final aggregation** on the reduce side combines them.

```
map side:    each partition computes partial counts per key   →  shuffle only the partials (tiny)
reduce side: sum the partials per key                         →  final result
```

> **Teradata bridge.** Exactly **AMP-local aggregation before BYNET redistribution.** It's why
> aggregations are *skew-resistant* (the hot key's millions of rows collapse to a handful of
> partial rows before they ever move) while **joins are not** (every row must travel — see §7).

---

## 7. Skew — the one hot partition (a hot AMP on a bad PI)

If one key value dominates (a mega-customer, a `NULL`, a default code), its hash bucket becomes a
giant partition, handled by **one task** that runs many times longer than its peers — a
**straggler**. The other tasks finish and idle; wall-clock = the slowest task. Pure shared-nothing
physics, identical to a **hot AMP on a skewed primary index**.

You *see* it in the Stages tab **Summary Metrics**: for **Duration** and **Shuffle Read Records**,
**Max ≫ Median**. We built an 80%-on-one-customer fact and watched one task carry 16 M rows while
199 peers read ~24 K each.

**Two non-obvious truths we learned the hard way:**

- **Aggregation skew mostly self-heals** (map-side partial aggregation, §6); **join skew does
  not** — a join must redistribute every row, so the whale's 16 M rows all land on one reduce task.
- **AQE detects skew by *bytes*, not rows** — specifically *compressed* shuffle bytes
  (`skewedPartitionThresholdInBytes`, default 256 MB, AND > `skewedPartitionFactor`×median, default
  5×). Our synthetic whale was 16 M rows but *hyper-compressible* (constant key, sequential ids),
  so its compressed size sat *under* the threshold and AQE ignored it until we lowered the bar.
  **Row-count skew and byte skew are different things; the thresholds are byte-based and
  data-dependent — you tune them to your data and verify in the UI.**

---

## 8. The three skew/shuffle fixes — and broadcast vs sort-merge join

When a join is slow it's almost always the shuffle. Three weapons, in order of preference:

| Fix | Mechanism | When | Reliability |
|---|---|---|---|
| **Broadcast join** | ship the small side to *every* executor; join map-side; **no shuffle of the big side** | one side fits in memory (`autoBroadcastJoinThreshold`, default 10 MB) | **deterministic** — no shuffle, no skew, no thresholds |
| **AQE skew-split** | runtime: detect the fat partition, split it into sub-tasks, replicate the matching side | both sides too big to broadcast | best-effort, **byte-threshold gated** — verify it engaged |
| **Salting** | add a random suffix to the hot key to spread it, aggregate in two passes | AQE not enough; need guaranteed control | manual but bulletproof |

**Broadcast vs sort-merge, in the plan** (the single most important join tuning):

- **`SortMergeJoin`** — both sides shuffled (two `Exchange hashpartitioning` nodes). The default
  for two large tables; where skew lives.
- **`BroadcastHashJoin … BuildRight`** — the small side (right) is built into a hash table and
  broadcast (one tiny `BroadcastExchange`); the big side has **no Exchange** — never shuffled, so
  skew is *impossible*. Spark picks this automatically when a side is under the threshold; `F.broadcast(df)` forces it.

> **Teradata bridge.** Broadcast ≈ **duplicating a small table to all AMPs** so the join goes
> AMP-local instead of redistributing the big table. AQE skew-split has **no Teradata equivalent** —
> runtime splitting of a skewed bucket is impossible when the PI distribution is frozen at design
> time. "Most slow joins are a missed broadcast" is as true here as "most slow Teradata joins are a
> bad/duplicated redistribution."

---

## 9. RDD vs DataFrame — and Catalyst

Two layers of API sit above the partitions:

- **RDD** (Resilient Distributed Dataset) — the original low-level abstraction: a distributed
  collection of objects + its lineage (so a lost partition can be recomputed). No schema, no
  optimizer.
- **DataFrame / Spark SQL** — a higher-level, columnar, *declarative* API. You write it; the
  **Catalyst** optimizer compiles it down to RDD operations, pushing filters, choosing joins,
  pruning columns. **Always write DataFrames/SQL**, not RDDs — you want the optimizer.

```
DataFrame / SQL   ← what you write (Catalyst optimizes it)
      ↓ compiles to
   RDD            ← partitions + tasks (you only touch this to inspect, e.g. .rdd.getNumPartitions())
```

> **Teradata bridge.** DataFrame/SQL ≈ the SQL + optimizer you know; RDD ≈ the physical
> AMP-step layer underneath. `.rdd` is for peeking at physical partitioning, the way you'd read an
> EXPLAIN to see redistribution steps.

---

## 10. AQE — the runtime re-planner

**Adaptive Query Execution** (`spark.sql.adaptive.enabled`, **on by default** since Spark 3.2) is
the optimizer that re-plans *during* execution using real shuffle statistics — not just the
up-front estimate. Three things it does, all of which we watched:

1. **Coalesce shuffle partitions** — collapse the blind 200 down to the right number for the actual
   data (our 50-group `groupBy`: 200 → 1). Only *merges small* partitions.
2. **Skew-join split** — detect a fat partition and split it (§7–8). Only *splits big* partitions.
   (Coalesce can't split; skew-split can't merge — they're complementary.)
3. **Switch join strategy** — e.g. flip a planned sort-merge to a broadcast if a side turns out
   small at runtime.

Because AQE re-plans mid-flight, it **submits a query as several jobs** (one per materialized query
stage) — which is why one `count` showed up as jobs 0,1,2 in the UI. And it's why `explain()` shows
`AdaptiveSparkPlan isFinalPlan=false`: the printed plan is the *pre-AQE* plan; the real one only
exists after execution.

> **Teradata bridge.** AQE ≈ a Teradata optimizer that could *re-optimize a query mid-flight* using
> the row counts it actually observed, instead of relying solely on `COLLECT STATISTICS` gathered
> beforehand. There's no real Teradata analog to runtime re-planning.

---

## 11. Ephemeral vs persistent partitioning — the bridge to Iceberg

Everything above is **ephemeral** partitioning — decided per query, gone when the query ends. The
*persistent* analog — partitioning that lives **on disk** so the *next* query starts already
well-distributed — is the real equivalent of **choosing a Teradata Primary Index**, and it lives in
**Iceberg** (Layer 3), not Spark:

| Teradata (on disk, fixed at design) | Iceberg (on disk, evolvable) | Spark (ephemeral, per query) |
|---|---|---|
| Primary Index (hash distribution) | `bucket(N, col)` partition transform | `repartition(n, col)` |
| Partitioned PI (PPI) | partition by `days(ts)`/`months(ts)` | — |
| sort key + `COLLECT STATISTICS` | write **sort order** | `sortWithinPartitions` |

If you **bucket** a fact by `customer_id` on disk, a later join on `customer_id` can skip the
shuffle entirely (a *bucketed/storage-partitioned join*) — exactly like a Teradata join on matching
PIs going AMP-local. **Design the physical layout once; reads stay cheap forever** — the lakehouse
version of PI design. (Hands-on for this lives in the CardCompass / big-plan ETL, not in this
fundamentals doc.)

---

## 12. Hard-won lessons (from the lab)

- **Each `SparkContext` is a separate "application" with its own Web UI**, on its own port starting
  at **4040** and incrementing (4041, 4042…). Our container's Thrift server squats on 4040, so the
  Jupyter kernel's app lands on 4041+. Find it with `spark.sparkContext.uiWebUrl` and publish that
  port. The UI is per-application, not per-host.
- **`explain()` is the pre-AQE plan** (`isFinalPlan=false`). To see what *actually* ran, read the
  SQL tab in the UI after execution.
- **Synthetic data lies about skew.** Constant/sequential columns compress to almost nothing, so
  byte-based skew thresholds never trip even at extreme row skew. Real-world skew is in the bytes.
- **Running apps show as "incomplete" in the History Server** until the `SparkContext` stops — use
  "Show incomplete applications." And the events dir must be on a persistent volume or completed
  logs vanish on container recreate.
- **Watch the Stages-tab Summary Metrics Min/Median/Max**, not just totals. The Max-vs-Median gap is
  how you *see* skew; the task count is how you *see* the shuffle-partition number.

---

## 13. Teradata cheat-sheet

| Teradata concept | Spark equivalent |
|---|---|
| Parsing Engine (parse, plan, dispatch) | **Driver** (`SparkSession`, scheduler) |
| AMP (owns a data slice, works in parallel) | **Executor** running **tasks** on **partitions** |
| BYNET redistribution | **Shuffle** (`Exchange hashpartitioning`) |
| AMP-local step (no redistribution) | **Stage** (narrow deps, pipelined within) |
| Redistribution step between local steps | **Stage boundary** (a shuffle) |
| Primary Index hash distribution | the partitioning of a DataFrame (per-op) / Iceberg `bucket()` (on disk) |
| Hash-bucket count for a redistribution | `spark.sql.shuffle.partitions` (default 200) |
| AMP-local aggregation before BYNET | **map-side partial aggregation** (two-phase agg) |
| Duplicate small table to all AMPs | **Broadcast join** (`BroadcastHashJoin`) |
| Hot AMP on a skewed PI | **straggler task** on a skewed shuffle partition |
| (no equivalent — PI is frozen) | **AQE skew-split** (runtime partition splitting) |
| `COLLECT STATISTICS` (gathered up front) | **AQE** runtime statistics + re-planning |
| EXPLAIN plan | `df.explain()` + the SQL tab in the Web UI |

---

*Lab specifics (versions, ports, the `local[*]` setup) are intentionally light — see the compose
and `spark-defaults.conf` under `lakehouse_fundamentals/docker/zeenie/` for the running wiring.
This doc captures the engine model; the next layer up is **orchestration** (Airflow) and the
**big-plan ETL** that puts all of this to work building a star schema.*
