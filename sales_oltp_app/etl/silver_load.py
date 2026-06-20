"""Silver load — turn the raw bronze append-log into clean, deduplicated, current-state entities.

For one run-date it takes that run's bronze slice, keeps the LATEST row per business key
(window by key, updated_at desc), conforms/cleans the columns (trim, casing, domain casing),
folds the category name into products, and upserts idempotently into `sales_silver.*` via
MERGE on the business key. Silver is the cleansed, business-key'd integration layer (Teradata
core) the gold star is built from. See docs/Sales_Warehouse_Design.md sec 4.

Run:  docker exec spark-iceberg spark-submit /home/iceberg/etl/silver_load.py --run-date 2026-06-19
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE = "nessie.sales_bronze"
SILVER = "nessie.sales_silver"

# business key per silver table (order_items is composite). product_categories is NOT a silver
# table — it is folded into products as category_name (the one denormalization, per design).
KEYS = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "stores": ["store_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "line_no"],
}


def table_exists(spark, ns, t):
    return spark.sql(f"SHOW TABLES IN {ns}").where(F.col("tableName") == t).limit(1).count() > 0


def latest_per_key(df, keys):
    # Defensive dedupe: the feed emits one row per key per run, but rank anyway so a re-fed
    # key within a slice collapses to its newest version.
    w = Window.partitionBy(*keys).orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rk", F.row_number().over(w)).where(F.col("_rk") == 1).drop("_rk")


def slice_latest(spark, t, run_date):
    df = spark.table(f"{BRONZE}.{t}").where(F.col("_run_date") == F.lit(run_date).cast("date"))
    return latest_per_key(df, KEYS[t])


def conform_customers(spark, run_date):
    s = slice_latest(spark, "customers", run_date)
    return s.select(
        "customer_id",
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.upper(F.trim("segment")).alias("segment"),
        F.initcap(F.trim("city")).alias("city"),
        F.upper(F.trim("state")).alias("state"),
        F.upper(F.trim("country")).alias("country"),
        "signup_date", "created_at", "updated_at",
    )


def conform_products(spark, run_date):
    s = slice_latest(spark, "products", run_date)
    # category name from current-state of the full categories history (a product fed today may
    # reference a category fed in an earlier run).
    cats = latest_per_key(spark.table(f"{BRONZE}.product_categories"), ["category_id"]) \
        .select("category_id", F.trim("category_name").alias("category_name"))
    return s.join(cats, "category_id", "left").select(
        "product_id",
        F.upper(F.trim("sku")).alias("sku"),
        F.trim("product_name").alias("product_name"),
        "category_id",
        "category_name",
        "unit_price",
        "active_flag",
        "created_at", "updated_at",
    )


def conform_stores(spark, run_date):
    s = slice_latest(spark, "stores", run_date)
    return s.select(
        "store_id",
        F.trim("store_name").alias("store_name"),
        F.upper(F.trim("channel")).alias("channel"),
        F.trim("region").alias("region"),
        F.initcap(F.trim("city")).alias("city"),
        F.upper(F.trim("state")).alias("state"),
        F.upper(F.trim("country")).alias("country"),
        "created_at", "updated_at",
    )


def conform_orders(spark, run_date):
    s = slice_latest(spark, "orders", run_date)
    return s.select(
        "order_id", "customer_id", "store_id", "order_ts", "order_date",
        F.upper(F.trim("status")).alias("status"),
        "order_total", "created_at", "updated_at",
    )


def conform_order_items(spark, run_date):
    s = slice_latest(spark, "order_items", run_date)
    return s.select(
        "order_id", "line_no", "product_id", "quantity", "unit_price",
        "discount_pct", "line_amount", "created_at", "updated_at",
    )


BUILDERS = {
    "customers": conform_customers,
    "products": conform_products,
    "stores": conform_stores,
    "orders": conform_orders,
    "order_items": conform_order_items,
}


def upsert(spark, df, t):
    fqn = f"{SILVER}.{t}"
    if not table_exists(spark, SILVER, t):
        df.limit(0).writeTo(fqn).using("iceberg").create()
    df.createOrReplaceTempView(f"incoming_{t}")
    on = " AND ".join(f"t.{k} = s.{k}" for k in KEYS[t])
    spark.sql(f"""
        MERGE INTO {fqn} t
        USING incoming_{t} s
        ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return df.count()


def main(run_date):
    spark = SparkSession.builder.appName(f"silver-load-{run_date}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER}")

    print(f"=== SILVER load for run_date={run_date} ===")
    for t in ["customers", "products", "stores", "orders", "order_items"]:
        df = BUILDERS[t](spark, run_date)
        n = upsert(spark, df, t)
        print(f"  {t:20} merged rows={n:>7}  -> {SILVER}.{t}")
    print("=== SILVER load complete ===")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", required=True, help="YYYY-MM-DD (the bronze slice to process)")
    main(p.parse_args().run_date)
