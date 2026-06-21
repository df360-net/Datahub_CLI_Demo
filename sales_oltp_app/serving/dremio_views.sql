-- Sales serving layer — Dremio semantic views over the gold star (DDL of record).
-- Applied idempotently by serving/build_dremio_views.py via Dremio's REST SQL API.
-- The `sales_curated` space is ensured by the builder before these run. CREATE OR REPLACE = re-runnable.
-- Physical gold lives in the `nessie` source: nessie.sales_gold.*  (Spark writes; Dremio reads).
-- Nessie is a VERSIONED catalog, so a view over it must pin the branch with `AT BRANCH main`.
-- See docs/Sales_Serving_Design.md.

-- ---------- curated star: thin pass-throughs (flexible self-service analysis) ----------
CREATE OR REPLACE VIEW sales_curated.dim_customer AS SELECT * FROM nessie.sales_gold.dim_customer AT BRANCH main;
CREATE OR REPLACE VIEW sales_curated.dim_product  AS SELECT * FROM nessie.sales_gold.dim_product  AT BRANCH main;
CREATE OR REPLACE VIEW sales_curated.dim_store    AS SELECT * FROM nessie.sales_gold.dim_store    AT BRANCH main;
CREATE OR REPLACE VIEW sales_curated.dim_date     AS SELECT * FROM nessie.sales_gold.dim_date     AT BRANCH main;
CREATE OR REPLACE VIEW sales_curated.fact_sales   AS SELECT * FROM nessie.sales_gold.fact_sales   AT BRANCH main;

-- ---------- streaming hello-world: Debezium CDC -> Spark -> Iceberg (nessie.sales_stream) ----------
-- See docs/05_Kafka_hello_world_design.md. Rows arrive via the streaming path, not the batch gold load.
CREATE OR REPLACE VIEW sales_curated.kafka_test AS SELECT * FROM nessie.sales_stream.kafka_test AT BRANCH main;

-- ---------- wide reporting OBT for Superset (AS-OF: joins on the surrogate keys) ----------
-- The fact's surrogate keys are already point-in-time-resolved, so a plain equi-join yields the
-- dimension version that was current at order time. No valid_from/valid_to or is_current logic.
-- year/quarter/month are reserved words in Dremio SQL -> quoted.
CREATE OR REPLACE VIEW sales_curated.vw_sales_report AS
SELECT
    f.order_id,
    f.line_no,
    f.status,
    f.order_date,
    d."year",
    d."quarter",
    d."month",
    d.month_name,
    d.day_name,
    d.is_weekend,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.segment,
    c.city  AS customer_city,
    c.state AS customer_state,
    p.product_id,
    p.sku,
    p.product_name,
    p.category_name,
    p.unit_price AS product_list_price,
    s.store_id,
    s.store_name,
    s.channel,
    s.region,
    s.city  AS store_city,
    s.state AS store_state,
    f.quantity,
    f.unit_price AS sale_unit_price,
    f.discount_pct,
    f.extended_amount
FROM sales_curated.fact_sales f
JOIN sales_curated.dim_date     d ON f.date_key     = d.date_key
JOIN sales_curated.dim_customer c ON f.customer_key = c.customer_key
JOIN sales_curated.dim_product  p ON f.product_key  = p.product_key
JOIN sales_curated.dim_store    s ON f.store_key    = s.store_key;
