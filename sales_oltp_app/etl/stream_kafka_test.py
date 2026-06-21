"""stream_kafka_test.py - Kafka hello-world: Debezium CDC -> Iceberg.

Reads the Debezium change-event topic for sales_oltp.kafka_test, keeps inserts + snapshot-reads
(op in 'c','r'), and appends the `after` row image into nessie.sales_stream.kafka_test.

Trigger.AvailableNow by default: drains whatever is on the topic right now, then stops - so it's
hand-drivable (insert in MySQL, re-run, the new row appears). The checkpoint makes each run pick
up only what's new (exactly-once incremental). Pass `stream` as arg 1 for a continuous query.

See lakehouse_fundamentals/docs/05_Kafka_hello_world_design.md.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

TOPIC = "zeenie.sales_oltp.kafka_test"
TABLE = "nessie.sales_stream.kafka_test"
CHECKPOINT = "/home/iceberg/warehouse/_chk/kafka_test"

# Only the slice of the Debezium envelope we need: op + the `after` row image.
after_schema = StructType([
    StructField("id", IntegerType()),
    StructField("note", StringType()),
    StructField("created_at", StringType()),
])
envelope_schema = StructType([
    StructField("op", StringType()),
    StructField("after", after_schema),
])

mode = sys.argv[1] if len(sys.argv) > 1 else "once"

spark = SparkSession.builder.appName("stream_kafka_test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.sales_stream")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE} (id int, note string, created_at string) USING iceberg")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .load())

# Debezium tombstones (null value) and deletes (op='d') fall out: from_json(null) -> null struct,
# and op not in ('c','r'). Updates (op='u') are ignored too - this hello-world only lands inserts.
rows = (raw.select(from_json(col("value").cast("string"), envelope_schema).alias("e"))
        .filter(col("e.op").isin("c", "r"))
        .select("e.after.id", "e.after.note", "e.after.created_at"))

writer = (rows.writeStream.format("iceberg")
          .outputMode("append")
          .option("checkpointLocation", CHECKPOINT))

if mode == "stream":
    q = writer.trigger(processingTime="5 seconds").toTable(TABLE)
else:
    q = writer.trigger(availableNow=True).toTable(TABLE)

q.awaitTermination()
