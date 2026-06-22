"""Declarative model of the Sales platform: every dataset's columns, the table-level lineage
edges between layers, and the Airflow DAG's task I/O. Schemas come verbatim from the OLTP DDL
(db/01_schema.sql) and the Spark etl jobs (silver_load/gold_dims/gold_fact) - the sources of truth.

A dataset is keyed by (layer, table). Layers map to (platform, namespace):
    mysql  -> (mysql,  sales_oltp)     bronze/silver/gold -> (spark, sales_{layer})
    dremio -> (dremio, sales_curated)
"""

# One logical platform; the three engines are represented as Database containers (by name), and
# the lakehouse's medallion layers are Schemas under it. layer -> (database, schema-or-None).
PLATFORM = "sales360"
LAYER_CONTAINER = {
    "mysql":  ("sales_oltp", None),            # MySQL OLTP -> tables directly in the database
    "bronze": ("sales_lakehouse", "sales_bronze"),
    "silver": ("sales_lakehouse", "sales_silver"),
    "gold":   ("sales_lakehouse", "sales_gold"),
    "dremio": ("sales_curated", None),         # Dremio views -> directly in the database
}
DATABASES = ("sales_oltp", "sales_lakehouse", "sales_curated")
LAKEHOUSE_SCHEMAS = ("sales_bronze", "sales_silver", "sales_gold")


def dataset_name(layer, table):
    """Dotted name under the instance, e.g. sales_lakehouse.sales_bronze.customers."""
    db, schema = LAYER_CONTAINER[layer]
    return ".".join([db] + ([schema] if schema else []) + [table])

# ---- OLTP (MySQL) ----  raw operational tables
_OLTP = {
    "customers":          ["customer_id", "first_name", "last_name", "email", "segment", "city", "state", "country", "signup_date", "created_at", "updated_at"],
    "products":           ["product_id", "sku", "product_name", "category_id", "unit_price", "active_flag", "created_at", "updated_at"],
    "product_categories": ["category_id", "category_name", "created_at", "updated_at"],
    "stores":             ["store_id", "store_name", "channel", "region", "city", "state", "country", "created_at", "updated_at"],
    "orders":             ["order_id", "customer_id", "store_id", "order_ts", "order_date", "status", "order_total", "created_at", "updated_at"],
    "order_items":        ["order_id", "line_no", "product_id", "quantity", "unit_price", "discount_pct", "line_amount", "created_at", "updated_at"],
}

# ---- silver ----  deduped-to-current + conformed; product_categories folded into products
_SILVER = {
    "customers":   ["customer_id", "first_name", "last_name", "email", "segment", "city", "state", "country", "signup_date"],
    "products":    ["product_id", "sku", "product_name", "category_id", "category_name", "unit_price", "active_flag"],
    "stores":      ["store_id", "store_name", "channel", "region", "city", "state", "country"],
    "orders":      ["order_id", "customer_id", "store_id", "order_ts", "order_date", "status", "order_total", "created_at", "updated_at"],
    "order_items": ["order_id", "line_no", "product_id", "quantity", "unit_price", "discount_pct", "line_amount", "created_at", "updated_at"],
}

_SCD2 = ["row_hash", "valid_from", "valid_to", "is_current"]
# ---- gold ----  SCD-2 star
_GOLD = {
    "dim_customer": ["customer_key", "customer_id", "first_name", "last_name", "email", "segment", "city", "state", "country", "signup_date"] + _SCD2,
    "dim_product":  ["product_key", "product_id", "sku", "product_name", "category_id", "category_name", "unit_price", "active_flag"] + _SCD2,
    "dim_store":    ["store_key", "store_id", "store_name", "channel", "region", "city", "state", "country"] + _SCD2,
    "dim_date":     ["date_key", "date", "year", "quarter", "month", "month_name", "day", "day_of_week", "day_name", "week_of_year", "is_weekend"],
    "fact_sales":   ["sales_key", "date_key", "customer_key", "product_key", "store_key", "order_id", "line_no", "status", "quantity", "unit_price", "discount_pct", "extended_amount", "order_date"],
}

# ---- dremio curated ----  pass-through views + the wide reporting OBT
_DREMIO = {
    "dim_customer":   _GOLD["dim_customer"],
    "dim_product":    _GOLD["dim_product"],
    "dim_store":      _GOLD["dim_store"],
    "dim_date":       _GOLD["dim_date"],
    "fact_sales":     _GOLD["fact_sales"],
    "vw_sales_report": ["order_id", "line_no", "status", "order_date", "year", "quarter", "month", "month_name", "day_name", "is_weekend",
                        "customer_id", "first_name", "last_name", "segment", "customer_city", "customer_state",
                        "product_id", "sku", "product_name", "category_name", "product_list_price",
                        "store_id", "store_name", "channel", "region", "store_city", "store_state",
                        "quantity", "sale_unit_price", "discount_pct", "extended_amount"],
}

# bronze mirrors the OLTP tables (raw typed append-log)
TABLES = {"mysql": _OLTP, "bronze": dict(_OLTP), "silver": _SILVER, "gold": _GOLD, "dremio": _DREMIO}


def datasets():
    """Yield (layer, table, columns) for every dataset."""
    for layer, tables in TABLES.items():
        for table, cols in tables.items():
            yield layer, table, cols


# ---- table-level lineage: downstream (layer,table) <- [upstream (layer,table), ...] ----
def _one_to_one(down_layer, up_layer, tables):
    return [((down_layer, t), [(up_layer, t)]) for t in tables]

LINEAGE = (
    # mysql -> bronze (1:1 mirror)
    _one_to_one("bronze", "mysql", _OLTP)
    # bronze -> silver
    + [
        (("silver", "customers"),   [("bronze", "customers")]),
        (("silver", "products"),    [("bronze", "products"), ("bronze", "product_categories")]),
        (("silver", "stores"),      [("bronze", "stores")]),
        (("silver", "orders"),      [("bronze", "orders")]),
        (("silver", "order_items"), [("bronze", "order_items")]),
    ]
    # silver -> gold
    + [
        (("gold", "dim_customer"), [("silver", "customers")]),
        (("gold", "dim_product"),  [("silver", "products")]),
        (("gold", "dim_store"),    [("silver", "stores")]),
        # dim_date is generated (no upstream)
        (("gold", "fact_sales"),   [("silver", "order_items"), ("silver", "orders"),
                                    ("gold", "dim_customer"), ("gold", "dim_product"), ("gold", "dim_store")]),
    ]
    # gold -> dremio (pass-throughs 1:1)
    + [(("dremio", v), [("gold", v)]) for v in ("dim_customer", "dim_product", "dim_store", "dim_date", "fact_sales")]
    # dremio OBT over the curated views
    + [(("dremio", "vw_sales_report"),
        [("dremio", "fact_sales"), ("dremio", "dim_customer"), ("dremio", "dim_product"),
         ("dremio", "dim_store"), ("dremio", "dim_date")])]
)

# ---- explicit column-level maps for the two NON-1:1 transforms ----
# downstream_col -> [(up_layer, up_table, up_col), ...].  Edges not listed here get auto-derived
# identity maps (downstream col <- same-named upstream col) in publish.py.
# gold.fact_sales (from gold_fact.py SELECT): surrogate keys resolved via the dim joins.
FACT_COLMAP = {
    "sales_key":       [("silver", "order_items", "order_id"), ("silver", "order_items", "line_no")],
    "date_key":        [("silver", "orders", "order_date")],
    "customer_key":    [("gold", "dim_customer", "customer_key")],
    "product_key":     [("gold", "dim_product", "product_key")],
    "store_key":       [("gold", "dim_store", "store_key")],
    "order_id":        [("silver", "order_items", "order_id")],
    "line_no":         [("silver", "order_items", "line_no")],
    "status":          [("silver", "orders", "status")],
    "quantity":        [("silver", "order_items", "quantity")],
    "unit_price":      [("silver", "order_items", "unit_price")],
    "discount_pct":    [("silver", "order_items", "discount_pct")],
    "extended_amount": [("silver", "order_items", "line_amount")],
    "order_date":      [("silver", "orders", "order_date")],
}
# dremio.vw_sales_report (from dremio_views.sql): wide OBT joining the curated views.
OBT_COLMAP = {
    "order_id":           [("dremio", "fact_sales", "order_id")],
    "line_no":            [("dremio", "fact_sales", "line_no")],
    "status":             [("dremio", "fact_sales", "status")],
    "order_date":         [("dremio", "fact_sales", "order_date")],
    "year":               [("dremio", "dim_date", "year")],
    "quarter":            [("dremio", "dim_date", "quarter")],
    "month":              [("dremio", "dim_date", "month")],
    "month_name":         [("dremio", "dim_date", "month_name")],
    "day_name":           [("dremio", "dim_date", "day_name")],
    "is_weekend":         [("dremio", "dim_date", "is_weekend")],
    "customer_id":        [("dremio", "dim_customer", "customer_id")],
    "first_name":         [("dremio", "dim_customer", "first_name")],
    "last_name":          [("dremio", "dim_customer", "last_name")],
    "segment":            [("dremio", "dim_customer", "segment")],
    "customer_city":      [("dremio", "dim_customer", "city")],
    "customer_state":     [("dremio", "dim_customer", "state")],
    "product_id":         [("dremio", "dim_product", "product_id")],
    "sku":                [("dremio", "dim_product", "sku")],
    "product_name":       [("dremio", "dim_product", "product_name")],
    "category_name":      [("dremio", "dim_product", "category_name")],
    "product_list_price": [("dremio", "dim_product", "unit_price")],
    "store_id":           [("dremio", "dim_store", "store_id")],
    "store_name":         [("dremio", "dim_store", "store_name")],
    "channel":            [("dremio", "dim_store", "channel")],
    "region":             [("dremio", "dim_store", "region")],
    "store_city":         [("dremio", "dim_store", "city")],
    "store_state":        [("dremio", "dim_store", "state")],
    "quantity":           [("dremio", "fact_sales", "quantity")],
    "sale_unit_price":    [("dremio", "fact_sales", "unit_price")],
    "discount_pct":       [("dremio", "fact_sales", "discount_pct")],
    "extended_amount":    [("dremio", "fact_sales", "extended_amount")],
}
EXPLICIT_COLMAP = {
    ("gold", "fact_sales"): FACT_COLMAP,
    ("dremio", "vw_sales_report"): OBT_COLMAP,
}

# ---- Airflow process lineage: ordered tasks with dataset I/O ----
_OLTP_T = list(_OLTP)
_BRONZE_T = list(_OLTP)
_SILVER_T = list(_SILVER)
_GOLD_T = list(_GOLD)
_DREMIO_T = list(_DREMIO)

AIRFLOW = [
    ("simulate_day",     "Generate one business day of OLTP activity",
        [], [("mysql", t) for t in _OLTP_T]),
    ("extract_feed",     "Incremental watermark extract to the landing area",
        [("mysql", t) for t in _OLTP_T], []),
    ("bronze",           "Land raw typed append-log (Spark -> Iceberg)",
        [("mysql", t) for t in _OLTP_T], [("bronze", t) for t in _BRONZE_T]),
    ("silver",           "Dedupe-to-current + conform (Spark -> Iceberg)",
        [("bronze", t) for t in _BRONZE_T], [("silver", t) for t in _SILVER_T]),
    ("gold_dims",        "Build SCD-2 dimensions + calendar (Spark -> Iceberg)",
        [("silver", "customers"), ("silver", "products"), ("silver", "stores")],
        [("gold", "dim_customer"), ("gold", "dim_product"), ("gold", "dim_store"), ("gold", "dim_date")]),
    ("gold_fact",        "Point-in-time fact resolution (Spark -> Iceberg)",
        [("silver", "order_items"), ("silver", "orders"), ("gold", "dim_customer"), ("gold", "dim_product"), ("gold", "dim_store")],
        [("gold", "fact_sales")]),
    ("dq_checks",        "Data-quality gate (grain / referential / reconciliation)",
        [("gold", t) for t in _GOLD_T], []),
    ("refresh_dremio",   "Build curated Dremio views over gold",
        [("gold", t) for t in _GOLD_T], [("dremio", t) for t in _DREMIO_T]),
    ("refresh_superset", "Refresh the Superset reporting dataset",
        [("dremio", "vw_sales_report")], []),
]
