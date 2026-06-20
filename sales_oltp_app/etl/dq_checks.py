"""DQ gate — assert the gold star's integrity; exit non-zero on any failure (fails the Airflow task).

Runs the design's data-quality checks (Sales_Warehouse_Design.md sec 10) as real assertions: grain
uniqueness, referential integrity (no orphan dim keys), reconciliation (fact line sums == order total),
and SCD-2 sanity (exactly one current row per business key). These are the integrity constraints the
OLTP enforced for us; in the lakehouse they are checks we run. (Wave 5: these become DataHub assertions.)

Run:  docker exec spark-iceberg spark-submit /home/iceberg/etl/dq_checks.py --run-date 2026-06-20
"""
import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "nessie.sales_silver"
GOLD = "nessie.sales_gold"


def main(run_date):
    spark = SparkSession.builder.appName(f"dq-checks-{run_date}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    fails = []

    def check(name, ok, detail=""):
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}  {detail}")
        if not ok:
            fails.append(name)

    f = spark.table(f"{GOLD}.fact_sales")

    # 1. grain — unique on (order_id, line_no)
    total = f.count()
    grain = f.select("order_id", "line_no").distinct().count()
    check("fact grain unique (order_id,line_no)", total == grain, f"rows={total} distinct={grain}")

    # 2. referential — no orphan dimension keys
    orphans = f.where(F.col("customer_key").isNull() | F.col("product_key").isNull() | F.col("store_key").isNull()).count()
    check("fact referential (no null dim keys)", orphans == 0, f"orphans={orphans}")

    # 3. reconciliation — per-order fact line sum == orders.order_total
    fo = f.groupBy("order_id").agg(F.sum("extended_amount").alias("fact_total"))
    o = spark.table(f"{SILVER}.orders").select("order_id", "order_total")
    mism = (fo.join(o, "order_id")
              .where(F.abs(F.col("fact_total") - F.col("order_total")) > F.lit(0.005)).count())
    check("reconciliation (line sum == order_total)", mism == 0, f"mismatches={mism}")

    # 4. SCD-2 sanity — exactly one current row per business key
    for dim, bk in [("dim_customer", "customer_id"), ("dim_product", "product_id"), ("dim_store", "store_id")]:
        multi = (spark.table(f"{GOLD}.{dim}").where("is_current")
                 .groupBy(bk).count().where("count > 1").count())
        check(f"{dim} one current row per {bk}", multi == 0, f"violations={multi}")

    spark.stop()
    if fails:
        print(f"=== DQ FAILED: {', '.join(fails)} ===")
        sys.exit(1)
    print("=== DQ PASSED ===")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", required=True)
    main(p.parse_args().run_date)
