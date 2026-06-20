"""sales_daily — orchestrate the whole Sales platform for one business day.

simulate_day -> extract_feed -> bronze -> silver -> gold_dims -> gold_fact -> dq_checks
             -> refresh_dremio -> refresh_superset

Python tasks run in this Airflow container; Spark tasks run via `docker exec spark-iceberg`.
A single {{ ds }} threads through every task as the run_date. See docs/Sales_Orchestration_Design.md.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SALES = "/opt/sales"                 # mounted Sales code (tools/serving/bi + .env)
SPARK = "sudo docker exec spark-iceberg spark-submit /home/iceberg/etl"

default_args = {"retries": 1}


def spark(job):
    return f"{SPARK}/{job}.py --run-date {{{{ ds }}}}"


with DAG(
    dag_id="sales_daily",
    description="End-to-end Sales platform: OLTP day -> feed -> medallion -> DQ -> serving/BI",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sales", "lakehouse"],
) as dag:
    # process #1 (the app): simulate a business day. NOT idempotent -> retry from extract, not here.
    simulate_day = BashOperator(
        task_id="simulate_day", retries=0,
        bash_command=f"cd {SALES}/tools && python generate_daily_activity.py daily --date {{{{ ds }}}}",
    )
    # process #2 (the feed): incremental extract by watermark, prefix pinned to the run_date.
    extract_feed = BashOperator(
        task_id="extract_feed",
        bash_command=f"cd {SALES}/tools && python extract_feed.py --run-date {{{{ ds }}}}",
    )
    bronze = BashOperator(task_id="bronze", bash_command=spark("bronze_load"))
    silver = BashOperator(task_id="silver", bash_command=spark("silver_load"))
    gold_dims = BashOperator(task_id="gold_dims", bash_command=spark("gold_dims"))
    gold_fact = BashOperator(task_id="gold_fact", bash_command=spark("gold_fact"))
    dq_checks = BashOperator(task_id="dq_checks", bash_command=spark("dq_checks"))
    # serving/BI are idempotent ensures (live views/datasets; new snapshots are queried automatically).
    refresh_dremio = BashOperator(
        task_id="refresh_dremio",
        bash_command=f"cd {SALES}/serving && python build_dremio_views.py",
    )
    refresh_superset = BashOperator(
        task_id="refresh_superset",
        bash_command=f"cd {SALES}/bi && python setup_superset.py",
    )

    (simulate_day >> extract_feed >> bronze >> silver >> gold_dims >> gold_fact
     >> dq_checks >> refresh_dremio >> refresh_superset)
