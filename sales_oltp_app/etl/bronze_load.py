"""Bronze load — land the Sales feed (raw CSV in MinIO landing) into Iceberg `sales_bronze.*`.

For one run-date it: reads s3://landing/sales/<run_date>/, validates each table's row count
against _manifest.json (control total), TYPES the all-string CSV columns explicitly, stamps ingest
metadata (_run_date/_source_file/_ingested_at), and writes idempotently by overwriting the
_run_date partition. Bronze is the immutable, append-accumulating raw log (one table per source).

Run:  docker exec spark-iceberg spark-submit /home/iceberg/etl/bronze_load.py --run-date 2026-06-19
Catalog + s3a config come from the mounted spark-defaults.conf. See docs/Sales_Warehouse_Design.md.
"""
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "landing"
NS = "nessie.sales_bronze"
TABLES = ["product_categories", "customers", "products", "stores", "orders", "order_items"]

# The "typing" step: CSV lands as all-strings; cast each column to its true type. Columns not
# listed stay string. Money is DECIMAL (never float). Bad casts become NULL (clean lab data;
# quarantine of uncastable rows is a future enhancement).
CASTS = {
    "product_categories": {"category_id": "int", "created_at": "timestamp", "updated_at": "timestamp"},
    "customers": {"customer_id": "bigint", "signup_date": "date", "created_at": "timestamp", "updated_at": "timestamp"},
    "products": {"product_id": "bigint", "category_id": "int", "unit_price": "decimal(10,2)", "active_flag": "int", "created_at": "timestamp", "updated_at": "timestamp"},
    "stores": {"store_id": "int", "created_at": "timestamp", "updated_at": "timestamp"},
    "orders": {"order_id": "bigint", "customer_id": "bigint", "store_id": "int", "order_ts": "timestamp", "order_date": "date", "order_total": "decimal(12,2)", "created_at": "timestamp", "updated_at": "timestamp"},
    "order_items": {"order_id": "bigint", "line_no": "int", "product_id": "bigint", "quantity": "int", "unit_price": "decimal(10,2)", "discount_pct": "decimal(5,4)", "line_amount": "decimal(12,2)", "created_at": "timestamp", "updated_at": "timestamp"},
}


def manifest_counts(spark, run_date):
    # Read directly via the Hadoop FS API: spark.read skips files starting with '_'.
    path = f"s3a://{BUCKET}/sales/{run_date}/_manifest.json"
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.open(hpath)
    try:
        content = jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()
    mf = json.loads(content)
    return {t["table"]: (t["rows"], t["file"]) for t in mf["tables"]}


def table_exists(spark, ns, t):
    return spark.sql(f"SHOW TABLES IN {ns}").where(F.col("tableName") == t).limit(1).count() > 0


def load_table(spark, run_date, t, expected_rows, src_file):
    path = f"s3a://{BUCKET}/sales/{run_date}/{src_file}"
    df = spark.read.option("header", True).csv(path)

    for col, typ in CASTS[t].items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(typ))

    df = (df
          .withColumn("_run_date", F.lit(run_date).cast("date"))
          .withColumn("_source_file", F.lit(src_file))
          .withColumn("_ingested_at", F.current_timestamp()))

    actual = df.count()
    if actual != expected_rows:
        raise ValueError(f"[{t}] control-total FAIL: read {actual} rows, manifest says {expected_rows}")

    fqn = f"{NS}.{t}"
    if table_exists(spark, NS, t):
        df.writeTo(fqn).overwritePartitions()          # idempotent: replace this run_date's slice
    else:
        df.writeTo(fqn).using("iceberg").partitionedBy(F.days("_run_date")).create()
    print(f"  {t:20} rows={actual:>7}  (manifest {expected_rows})  -> {fqn}")


def main(run_date):
    spark = SparkSession.builder.appName(f"bronze-load-{run_date}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")

    counts = manifest_counts(spark, run_date)
    print(f"=== BRONZE load for run_date={run_date} ===")
    for t in TABLES:
        if t not in counts:
            raise ValueError(f"manifest missing table {t}")
        expected, src_file = counts[t]
        load_table(spark, run_date, t, expected, src_file)
    print("=== BRONZE load complete ===")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", required=True, help="YYYY-MM-DD (the landing prefix / extraction date)")
    main(p.parse_args().run_date)
