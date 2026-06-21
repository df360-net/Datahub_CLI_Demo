# Kafka Hello-World — End-to-End CDC Streaming Design

A minimal, hand-drivable streaming pipeline that proves the whole chain: a row inserted in
MySQL appears, seconds later, as a queryable row in Dremio — with no batch job, no manual
extract. One table, one topic, one streaming query. The point is to **see** streaming work
end to end and to internalize the moving parts, not to be production-grade.

## The flow

```
MySQL.kafka_test ─Debezium─► Kafka topic ─Spark Structured Streaming─►
   Iceberg  nessie.sales_stream.kafka_test ─Dremio view─► sales_curated.kafka_test
```

Five hops, each a layer you already know in its batch form:

1. **MySQL → Debezium** — CDC taps MySQL's binlog (its native replication log).
2. **Debezium → Kafka** — change events published to a topic, one per table.
3. **Kafka → Spark** — Structured Streaming subscribes to the topic, parses the change event.
4. **Spark → Iceberg** — writes rows into an Iceberg table in the `nessie` catalog.
5. **Iceberg → Dremio** — a Dremio view in the `sales_curated` space serves it for query.

The critical correction baked into this design: **Spark writes Iceberg (`nessie` catalog), not
the Dremio space.** `sales_curated` is Dremio's serving layer (VDS views); it *reads* Iceberg,
it is never a write target. Same storage-vs-serving split as the batch medallion — streaming
changes the *transport*, not the layering.

## Components

| # | Piece | Where it runs | Talks to |
|---|---|---|---|
| 0 | binlog enabled + Debezium user | MySQL @ Zeenie (192.168.0.21:3306, standalone) | — |
| 1 | `debezium/connect` (Kafka Connect worker + Debezium plugins) | container on `iceberg_net` | `kafka:9092`, MySQL:3306 |
| 2 | `kafka_test` table + posted connector config | MySQL / Connect REST API | — |
| 3 | Spark Structured Streaming job (+ kafka package) | `spark-iceberg` container | `kafka:9092`, `nessie` |
| 4 | Iceberg table `nessie.sales_stream.kafka_test` | MinIO (written by Spark) | — |
| 5 | manual `INSERT` (the driver) | MySQL | — |
| 6 | watch the topic | Redpanda Console (http://192.168.0.21:8085) | — |
| 7 | Dremio view `sales_curated.kafka_test` | Dremio | `nessie` |

Kafka (single-broker KRaft) and Redpanda Console already exist on Zeenie behind the
`streaming` compose profile. This design adds Debezium/Connect and the Spark streaming job.

## Step 0 — MySQL binlog prerequisites (the gate)

Debezium reads the binlog, so nothing downstream works until these are true. All are native
MySQL settings — no add-ons. MySQL is **standalone on Zeenie** (not a container), so these go
in its config file and require a MySQL restart.

| Setting | Required value | Why |
|---|---|---|
| `log_bin` | ON | the binlog must exist |
| `binlog_format` | `ROW` | Debezium needs row-level changes, not statements |
| `binlog_row_image` | `FULL` | to get full `before`/`after` row images |
| `server_id` | any unique int | Debezium registers as a replica; replicas need distinct ids |
| (optional) `gtid_mode` | ON | cleaner restart positioning; not required for hello-world |

Plus a replication user for Debezium:

```sql
CREATE USER 'debezium'@'%' IDENTIFIED BY '<pw>';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
```

- `REPLICATION SLAVE` + `REPLICATION CLIENT` — read the binlog stream and its position.
- `SELECT` — read existing rows during the initial snapshot.
- `RELOAD` — brief global read lock to get a consistent snapshot start point.

**Verify before proceeding:** `SHOW VARIABLES LIKE 'log_bin';` → `ON`, and
`SHOW VARIABLES LIKE 'binlog_format';` → `ROW`. If binlog is off, fix the foundation first.

## Step 1 — Debezium Connect container

The Debezium image is a **Kafka Connect worker** with the Debezium plugins baked in. It is
*not* configured by a static file — it boots, joins the broker, and waits for connector
configs posted over its REST API. It auto-creates three internal Kafka topics for its own
state (config / offsets / status).

Compose service (sketch), on `iceberg_net`, behind the `streaming` profile:

```yaml
connect:
  image: quay.io/debezium/connect:3.1.3.Final
  environment:
    BOOTSTRAP_SERVERS: kafka:9092
    GROUP_ID: 1
    CONFIG_STORAGE_TOPIC: _connect_configs
    OFFSET_STORAGE_TOPIC: _connect_offsets
    STATUS_STORAGE_TOPIC: _connect_statuses
  ports: ["8083:8083"]      # Connect REST API
```

(Redpanda Console will then show **Connect** as configured and surface this worker + its
connectors.)

## Step 2 — the table and the connector

Create the source table:

```sql
CREATE TABLE sales_oltp.kafka_test (
  id        INT PRIMARY KEY,
  note      VARCHAR(200),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Register the Debezium MySQL connector by `POST`ing JSON to `http://...:8083/connectors`:

```json
{
  "name": "kafka-test-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "192.168.0.21",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "<pw>",
    "database.server.id": "184054",
    "topic.prefix": "zeenie",
    "database.include.list": "sales_oltp",
    "table.include.list": "sales_oltp.kafka_test",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-history.kafka_test"
  }
}
```

The resulting topic is named `<topic.prefix>.<database>.<table>` → **`zeenie.sales_oltp.kafka_test`**.

## Step 3 — the Spark Structured Streaming job

Spark *does* have a first-class Kafka source, but the base image doesn't ship the jar — the
job needs the package (matched to the Spark version):

```
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5
```

The topic carries Debezium's **envelope**, not plain rows:

```json
{ "op": "u",
  "before": { "id": 1, "note": "old" },
  "after":  { "id": 1, "note": "new" },
  "source": { "table": "kafka_test", "ts_ms": ..., "file": "...", "pos": ... } }
```

`op`: `c`=create, `u`=update, `d`=delete, `r`=snapshot-read. For hello-world, keep it simple —
**filter `op IN ('c','r')` and take `after`** as the row to write. (Updates/deletes are where
this later grows toward SCD-2: the `before`/`after` pair is exactly the change signal a
close-then-open MERGE consumes.)

Job shape:

```python
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "zeenie.sales_oltp.kafka_test")
       .option("startingOffsets", "earliest")
       .load())

# value is JSON bytes -> parse envelope -> keep inserts/snapshot -> select `after.*`
rows = parse_debezium_envelope(raw)          # filter op in ('c','r'), explode `after`

q = (rows.writeStream.format("iceberg")
     .outputMode("append")
     .option("checkpointLocation", "s3://warehouse/_chk/kafka_test")
     .toTable("nessie.sales_stream.kafka_test")
     .start())
```

A streaming query **requires a checkpoint location** (it stores Kafka offsets + progress, so a
restart resumes exactly-once rather than reprocessing or skipping). `nessie.sales_stream` is a
**fresh namespace** so the experiment never touches bronze/silver/gold.

**Honest caveat (not a blocker):** inserting one row at a time → one micro-batch → one tiny
Iceberg data file each. That's the small-files problem from Module 8. Completely fine for
hello-world; production streaming-into-Iceberg adds compaction (`rewrite_data_files`). Do not
optimize it now — just know the trade is there.

## Step 4-5 — drive it by hand

Insert one row at a time in MySQL:

```sql
INSERT INTO sales_oltp.kafka_test (id, note) VALUES (1, 'first streamed row');
```

## Step 6 — watch the topic (Redpanda Console)

Open http://192.168.0.21:8085 → **Topics → `zeenie.sales_oltp.kafka_test` → Messages**. The
`INSERT` shows up as a pretty-printed Debezium change event — `op: "c"`, `after` populated,
`source` metadata. This is the "see streaming with your own eyes" moment.

## Step 7 — query the result (Dremio)

Add a pass-through view in the `sales_curated` space over the Iceberg table:

```sql
CREATE OR REPLACE VIEW sales_curated.kafka_test AS
SELECT * FROM nessie.sales_stream.kafka_test AT BRANCH main;
```

`SELECT * FROM sales_curated.kafka_test` returns the row you inserted in MySQL seconds ago.
Insert another in MySQL, re-query in Dremio, watch it appear. End to end, hand-driven.

## What this proves (and what it sets up)

Proves the streaming substrate works: binlog → CDC → topic → stream → Iceberg → serve, with
every hop independently observable (Redpanda for the topic, the Iceberg snapshot for the
write, Dremio for the serve). It's the same medallion layering you built in batch — the only
change is the transport from "daily extract" to "continuous tail of the binlog."

Sets up the real lesson: once `op IN ('c','r')` works, handling `u`/`d` turns this into
**streaming SCD-2** — the Debezium `before`/`after` envelope is precisely the change signal
that drives a close-then-open MERGE. CDC and SCD-2 are made for each other: Debezium hands you
the change, SCD-2 records its history.

## Status

**IMPLEMENTED & VERIFIED (2026-06-21).** Full chain proven end to end: a row inserted in
MySQL flows through Debezium → Kafka → Spark → Iceberg → Dremio and returns from
`sales_curated.kafka_test`.

- Step 0 — MySQL binlog already ON (`ROW`/`FULL`, `server_id=1`); no change needed.
- Debezium user + `kafka_test` table — `sales_oltp_app/db/04_streaming_debezium.sql`.
- `connect` (quay.io/debezium/connect:3.1.3.Final) — compose `streaming` profile, REST on :8083.
- Connector config — `sales_oltp_app/streaming/debezium_kafka_test.json`; topic
  `zeenie.sales_oltp.kafka_test`.
- Spark streaming job — `sales_oltp_app/etl/stream_kafka_test.py`. **Trigger.AvailableNow**
  (`once`) by default: drains the topic and stops, so it's hand-drivable; the checkpoint makes
  each run incremental. Pass `stream` for a continuous query.
- Dremio view — added to `sales_oltp_app/serving/dremio_views.sql`, applied by the builder.

**Hand-driven loop:** `INSERT` in MySQL → re-run the Spark job (`spark-submit ... --packages
org.apache.iceberg:iceberg-nessie:1.8.1,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5
/home/iceberg/etl/stream_kafka_test.py once`) → query `sales_curated.kafka_test` in Dremio.

**Next iteration (toward streaming SCD-2):** handle `op='u'`/`op='d'` using the Debezium
`before`/`after` pair to drive a close-then-open MERGE; and run the job in `stream` mode for
continuous (vs hand-driven) operation.
