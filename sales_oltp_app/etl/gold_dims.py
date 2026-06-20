"""Gold dimensions — build the conformed SCD-2 dimensions + the generated date calendar.

dim_date is a static generated calendar (createOrReplace, deterministic). dim_customer/product/store
are SCD-2: each carries valid_from/valid_to/is_current/row_hash and a versioned hash surrogate key
xxhash64(<business_id>, valid_from). First load = onboarding: every key is v1 with valid_from at the
1900-01-01 sentinel so all historical facts resolve to v1. Daily load = the two-step MERGE: step 1
CLOSES the current row of every tracked-attribute change (is_current=false, valid_to=run_date), step 2
OPENS a fresh version for every new + changed key (valid_from=run_date). See design sec 5-7.

gold_dims runs BEFORE gold_fact (the fact resolves dim keys point-in-time, so dims must be current).
Run:  docker exec spark-iceberg spark-submit /home/iceberg/etl/gold_dims.py --run-date 2026-06-19
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "nessie.sales_silver"
GOLD = "nessie.sales_gold"

SENTINEL = "1900-01-01"   # onboarding valid_from: all backfilled history resolves to v1
OPEN = "9999-12-31"       # open-ended valid_to for the current version

# name -> silver source, business key, attributes carried, and the tracked subset that triggers a
# new SCD-2 version (a change in any tracked attr = new version; untracked attrs ride along).
DIMS = {
    "customer": {
        "src": "customers", "bk": "customer_id",
        "attrs": ["first_name", "last_name", "email", "segment", "city", "state", "country", "signup_date"],
        "tracked": ["segment", "city", "state"],
    },
    "product": {
        "src": "products", "bk": "product_id",
        "attrs": ["sku", "product_name", "category_id", "category_name", "unit_price", "active_flag"],
        "tracked": ["unit_price", "product_name", "category_name"],
    },
    "store": {
        "src": "stores", "bk": "store_id",
        "attrs": ["store_name", "channel", "region", "city", "state", "country"],
        "tracked": ["region", "channel", "store_name"],
    },
}


def table_exists(spark, ns, t):
    return spark.sql(f"SHOW TABLES IN {ns}").where(F.col("tableName") == t).limit(1).count() > 0


def finalize(df, key_col, bk, attrs):
    # one canonical column order, shared by onboarding-create and daily-append (append is name-based,
    # but keeping order identical avoids any surprise).
    cols = [key_col, bk] + attrs + ["row_hash", "valid_from", "valid_to", "is_current"]
    return df.select(*cols)


def open_versions(df, key_col, bk, attrs, batch):
    return finalize(
        df.withColumn("valid_from", F.lit(batch).cast("date"))
          .withColumn("valid_to", F.lit(OPEN).cast("date"))
          .withColumn("is_current", F.lit(True))
          .withColumn(key_col, F.xxhash64(F.col(bk), F.lit(batch).cast("date"))),
        key_col, bk, attrs,
    )


def load_dim(spark, name, spec, run_date):
    src, bk, attrs, tracked = spec["src"], spec["bk"], spec["attrs"], spec["tracked"]
    key_col = f"{name}_key"
    fqn = f"{GOLD}.dim_{name}"

    incoming = (spark.table(f"{SILVER}.{src}")
                .select(bk, *attrs)
                .withColumn("row_hash", F.xxhash64(*[F.col(c) for c in tracked])))

    if not table_exists(spark, GOLD, f"dim_{name}"):
        v1 = open_versions(incoming, key_col, bk, attrs, SENTINEL)
        v1.writeTo(fqn).using("iceberg").create()
        print(f"  dim_{name:9} ONBOARD v1 rows={v1.count():>7} (valid_from={SENTINEL})  -> {fqn}")
        return

    cur = (spark.table(fqn).where(F.col("is_current"))
           .select(F.col(bk), F.col("row_hash").alias("cur_hash")))
    j = incoming.join(cur, bk, "left")
    changed = j.where(F.col("cur_hash").isNotNull() & (F.col("cur_hash") != F.col("row_hash")))
    needs_version = j.where(F.col("cur_hash").isNull() | (F.col("cur_hash") != F.col("row_hash")))

    n_changed = changed.count()
    opens = open_versions(needs_version.select(*incoming.columns), key_col, bk, attrs, run_date)
    n_open = opens.count()

    # STEP 1 — close the current row of every changed key.
    changed.select(bk).createOrReplaceTempView(f"close_{name}")
    spark.sql(f"""
        MERGE INTO {fqn} t
        USING close_{name} s
        ON t.{bk} = s.{bk} AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET t.is_current = false, t.valid_to = DATE'{run_date}'
    """)
    # STEP 2 — open a fresh current version for every new + changed key.
    opens.writeTo(fqn).append()
    print(f"  dim_{name:9} closed={n_changed:>5}  opened={n_open:>5} (new+changed, valid_from={run_date})  -> {fqn}")


def load_dim_date(spark):
    fqn = f"{GOLD}.dim_date"
    cal = spark.sql("""
        SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) AS date
    """)
    d = cal.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
    d.writeTo(fqn).using("iceberg").createOrReplace()
    print(f"  dim_date     generated rows={d.count():>7} (2020-01-01..2030-12-31)  -> {fqn}")


def main(run_date):
    spark = SparkSession.builder.appName(f"gold-dims-{run_date}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD}")

    print(f"=== GOLD dims for run_date={run_date} ===")
    load_dim_date(spark)
    for name, spec in DIMS.items():
        load_dim(spark, name, spec, run_date)
    print("=== GOLD dims complete ===")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", required=True, help="YYYY-MM-DD (the batch effective date for new versions)")
    main(p.parse_args().run_date)
