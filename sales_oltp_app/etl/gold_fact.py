"""Gold fact — build fact_sales at order-line grain with point-in-time dimension resolution.

Joins silver.order_items to silver.orders, then resolves customer_key/product_key/store_key to the
dimension version that was CURRENT AT THE ORDER'S BUSINESS DATE (order_date in [valid_from, valid_to)),
not whatever is current now. date_key derives from order_date; order_id/line_no/status ride as
degenerate dimensions; measures (quantity, extended_amount) are additive. Upserts idempotently via
MERGE on the grain (order_id, line_no): new lines INSERT with point-in-time keys, re-fed orders only
propagate status (existing fact rows are never re-pointed). COW, partitioned by days(order_date).
See design sec 5.1, 7.2, 8. Runs AFTER gold_dims.

Run:  docker exec spark-iceberg spark-submit /home/iceberg/etl/gold_fact.py --run-date 2026-06-19
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "nessie.sales_silver"
GOLD = "nessie.sales_gold"
FQN = f"{GOLD}.fact_sales"


def table_exists(spark, ns, t):
    return spark.sql(f"SHOW TABLES IN {ns}").where(F.col("tableName") == t).limit(1).count() > 0


def build_desired(spark):
    # Point-in-time star: each line's dim keys resolve to the version valid at order_date. LEFT joins
    # so an unresolved FK surfaces as NULL (caught below) rather than silently dropping the row.
    return spark.sql(f"""
        SELECT
            xxhash64(oi.order_id, oi.line_no)                  AS sales_key,
            cast(date_format(o.order_date, 'yyyyMMdd') AS int) AS date_key,
            dc.customer_key,
            dp.product_key,
            ds.store_key,
            oi.order_id,
            oi.line_no,
            o.status,
            oi.quantity,
            oi.unit_price,
            oi.discount_pct,
            oi.line_amount                                     AS extended_amount,
            o.order_date
        FROM {SILVER}.order_items oi
        JOIN {SILVER}.orders o
            ON oi.order_id = o.order_id
        LEFT JOIN {GOLD}.dim_customer dc
            ON o.customer_id = dc.customer_id
           AND o.order_date >= dc.valid_from AND o.order_date < dc.valid_to
        LEFT JOIN {GOLD}.dim_product dp
            ON oi.product_id = dp.product_id
           AND o.order_date >= dp.valid_from AND o.order_date < dp.valid_to
        LEFT JOIN {GOLD}.dim_store ds
            ON o.store_id = ds.store_id
           AND o.order_date >= ds.valid_from AND o.order_date < ds.valid_to
    """)


def assert_no_orphans(desired):
    orphans = desired.where(
        F.col("customer_key").isNull() | F.col("product_key").isNull() | F.col("store_key").isNull()
    ).count()
    if orphans:
        raise ValueError(f"referential DQ FAIL: {orphans} fact rows have an unresolved dimension key")


def main(run_date):
    spark = SparkSession.builder.appName(f"gold-fact-{run_date}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"=== GOLD fact for run_date={run_date} ===")
    desired = build_desired(spark)
    assert_no_orphans(desired)

    if not table_exists(spark, GOLD, "fact_sales"):
        desired.writeTo(FQN).using("iceberg").partitionedBy(F.days("order_date")).create()
        spark.sql(f"ALTER TABLE {FQN} WRITE ORDERED BY customer_key")
        print(f"  fact_sales   ONBOARD rows={desired.count():>7}  -> {FQN}")
    else:
        desired.createOrReplaceTempView("fact_in")
        spark.sql(f"""
            MERGE INTO {FQN} t
            USING fact_in s
            ON t.order_id = s.order_id AND t.line_no = s.line_no
            WHEN MATCHED THEN UPDATE SET t.status = s.status
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"  fact_sales   MERGED desired={desired.count():>7} (insert new lines, propagate status)  -> {FQN}")

    print("=== GOLD fact complete ===")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    main(p.parse_args().run_date)
