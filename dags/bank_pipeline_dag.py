"""
bank_pipeline_dag.py — Airflow DAG for Bank Transactions ETL
==============================================================
Purpose:
    Orchestrates the end-to-end data pipeline:
        Task 1: Extract (Landing Zone → Bronze)
        Task 2: Archive processed files (Data Lifecycle Management)
        Task 3: Transform (Bronze → Gold Star Schema)
        Task 4: Load (Gold → Snowflake via spark-submit)

"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ── Default arguments applied to all tasks ──
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    # Logical retries: if a task fails, Airflow retries once after 5 minutes.
    # This handles transient failures (network blips, HDFS not yet ready, etc.)
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Bank_Transactions_ETL_Pipeline",
    default_args=default_args,
    description="Production-ready End-to-End Data Engineering Pipeline for Bank Transactions",
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bank", "etl", "production"],
) as dag:


    # ──────────────────────────────────────────────────────────────────
    # Task 1: Extract — Landing Zone → Bronze (HDFS Parquet)
    # ──────────────────────────────────────────────────────────────────
    # Reads new JSONL files from the streaming landing zone, enforces
    # a strict schema, adds ingestion_timestamp, and appends to Bronze.
    extract_landing_to_bronze = BashOperator(
        task_id="Extract_Landing_To_Bronze",
        bash_command="docker exec spark-jupyter spark-submit /home/jovyan/work/extract.py",
    )

    # ──────────────────────────────────────────────────────────────────
    # Task 2: Archive Processed Files (Data Lifecycle Management)
    # ──────────────────────────────────────────────────────────────────
    # Physically moves the processed JSONL files from the landing zone
    # to a timestamped archive folder. This prevents reprocessing on
    # the next DAG run and maintains an audit trail.
    archive_processed_files = BashOperator(
        task_id="Archive_Processed_Files",
        bash_command=(
            'docker exec spark-jupyter bash -c "'
            "ARCHIVE_DIR=/home/jovyan/data/streaming_landing_zone/archived/$(date +%Y%m%d_%H%M%S) && "
            "mkdir -p \\$ARCHIVE_DIR && "
            "mv /home/jovyan/data/streaming_landing_zone/*.jsonl \\$ARCHIVE_DIR/ && "
            'echo Archived to \\$ARCHIVE_DIR"'
        ),
    )

    # ──────────────────────────────────────────────────────────────────
    # Task 3: Transform — Bronze → Gold (Star Schema)
    # ──────────────────────────────────────────────────────────────────
    # Reads Bronze Parquet, builds dim_time, dim_account, dim_type,
    # and fact_transactions with role-playing dimension pattern.
    transform_bronze_to_gold = BashOperator(
        task_id="Transform_Bronze_To_Gold",
        bash_command="docker exec spark-jupyter spark-submit /home/jovyan/work/transform.py",
    )

    # ──────────────────────────────────────────────────────────────────
    # Task 4: Load — Gold → Snowflake (spark-submit, no pip install)
    # ──────────────────────────────────────────────────────────────────
    # Executes load.py directly via spark-submit with the Snowflake
    # Spark Connector package. All Python dependencies are pre-installed
    # in the Docker image (Dockerfile.spark). No runtime pip installs.
    load_gold_to_snowflake = BashOperator(
        task_id="Load_Gold_To_Snowflake",
        bash_command=(
            "docker exec spark-jupyter spark-submit "
            "--packages net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4 "
            "/home/jovyan/work/load.py"
        ),
    )

    # ──────────────────────────────────────────────────────────────────
    # Task Dependencies (linear pipeline)
    # ──────────────────────────────────────────────────────────────────
    (
        extract_landing_to_bronze
        >> archive_processed_files
        >> transform_bronze_to_gold
        >> load_gold_to_snowflake
    )
