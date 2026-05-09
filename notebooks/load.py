"""
load.py — Gold Layer → Snowflake (Atomic Swap + Idempotent Append)
====================================================================
Purpose:
    Loads the Gold Star Schema from HDFS into Snowflake using the official
    Snowflake Spark Connector.

    DIMENSIONS (dim_account, dim_type, dim_time):
        Atomic Swap pattern for zero-downtime deployment:
        1. Write data to a _TEMP staging table
        2. ALTER TABLE ... SWAP WITH ... (atomic metadata-only operation)
        3. DROP the old table (now renamed to _TEMP)

    FACT TABLE (fact_transactions):
        Idempotent Incremental Append:
        1. Query the current high-water mark (MAX ingestion_timestamp)
        2. Filter Gold data to only rows AFTER the high-water mark
        3. Append net-new rows — safe for retries (no duplicates)

Anti-patterns avoided:
    ✅ Atomic Swap — zero downtime for dimension refreshes
    ✅ High-water mark — idempotent incremental fact loading
    ✅ No TRUNCATE + reload — prevents data loss windows
    ✅ Snowflake Spark Connector — no CSV intermediate step
    ✅ Credentials from environment variables — no hardcoded secrets

Run via:
    spark-submit --packages net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4 \\
        /home/jovyan/work/load.py
"""

import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


# ── HDFS requires the HADOOP_USER_NAME to be set for read permissions ──
os.environ["HADOOP_USER_NAME"] = "root"


# ==========================================================================
# Snowflake DDL Statements
# ==========================================================================

CREATE_DIM_ACCOUNT = """
CREATE TABLE IF NOT EXISTS DIM_ACCOUNT (
    ACCOUNT_NAME  VARCHAR(50),
    ACCOUNT_ID    BIGINT
)
"""

CREATE_DIM_TYPE = """
CREATE TABLE IF NOT EXISTS DIM_TYPE (
    TYPE     VARCHAR(30),
    TYPE_ID  BIGINT
)
"""

CREATE_DIM_TIME = """
CREATE TABLE IF NOT EXISTS DIM_TIME (
    YEAR          INT,
    MONTH         INT,
    DAY           INT,
    DAY_OF_WEEK   INT,
    WEEKEND_FLAG  INT,
    TIME_ID       BIGINT
)
"""

CREATE_FACT_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS FACT_TRANSACTIONS (
    TRANSACTION_ID     BIGINT,
    STEP               INT,
    TYPE_ID            BIGINT,
    TIME_ID            BIGINT,
    AMOUNT             DOUBLE,
    ORIG_ACCOUNT_ID    BIGINT,
    OLDBALANCEORG      DOUBLE,
    NEWBALANCEORIG     DOUBLE,
    DEST_ACCOUNT_ID    BIGINT,
    OLDBALANCEDEST     DOUBLE,
    NEWBALANCEDEST     DOUBLE,
    ISFRAUD            INT,
    ISFLAGGEDFRAUD     INT,
    INGESTION_TIMESTAMP TIMESTAMP_NTZ
)
"""


def _get_sf_options() -> dict:
    """
    Build the Snowflake connection options dict from environment variables.
    Raises KeyError immediately if any required variable is missing.
    """
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]
    for var in required_vars:
        if var not in os.environ:
            raise EnvironmentError(
                f"[Load] Missing required environment variable: {var}"
            )

    return {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
        "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
    }


def _atomic_swap_dimension(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    sf_options: dict,
) -> None:
    """
    Atomic Swap pattern for dimension tables (zero-downtime refresh).
    """
    temp_table = f"{table_name}_TEMP"

    print(f"[Load] Atomic Swap: Writing to staging table {temp_table}...")

    # Step 1: Write the full dimension to the TEMP table
    (
        df.write.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", temp_table)
        .mode("overwrite")
        .save()
    )

    # Step 2 & 3: SWAP and cleanup
    print(f"[Load] Atomic Swap: Executing SWAP {table_name} ↔ {temp_table}...")

    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    cursor = conn.cursor()
    try:
        # Ensure the production table exists for SWAP to work
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {temp_table}")

        # Atomic swap: instantaneous metadata-only operation
        cursor.execute(f"ALTER TABLE {table_name} SWAP WITH {temp_table}")
        print(f"[Load] Atomic Swap: ✅ {table_name} swapped successfully.")

        # Drop the old data (now in _TEMP after the swap)
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        print(f"[Load] Atomic Swap: Cleaned up {temp_table}.")
    except Exception as e:
        # Rollback: drop the temp table if swap fails
        print(f"[Load] Atomic Swap: ❌ SWAP failed for {table_name}: {e}")
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        raise
    finally:
        cursor.close()
        conn.close()

def _get_high_water_mark(sf_options: dict) -> str:
    """
    Query Snowflake for the maximum ingestion_timestamp in FACT_TRANSACTIONS.
    Returns the timestamp as an ISO string, or None if the table is empty
    or doesn't exist yet.
    """
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT MAX(INGESTION_TIMESTAMP) FROM FACT_TRANSACTIONS"
        )
        result = cursor.fetchone()
        if result and result[0]:
            hwm = str(result[0])
            print(f"[Load] High-water mark: {hwm}")
            return hwm
        else:
            print("[Load] No existing fact data. Full initial load.")
            return None
    except snowflake.connector.errors.ProgrammingError:
        # Table doesn't exist yet — first run
        print("[Load] FACT_TRANSACTIONS does not exist yet. Full initial load.")
        return None
    finally:
        cursor.close()
        conn.close()


def _ensure_fact_table_exists() -> None:
    """Create the FACT_TRANSACTIONS table if it doesn't exist."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    cursor = conn.cursor()
    try:
        cursor.execute(CREATE_FACT_TRANSACTIONS)
    finally:
        cursor.close()
        conn.close()


def load_gold_to_snowflake() -> None:
    """
    Main entry point: read Gold Star Schema from HDFS and load into Snowflake.
    """

    # ── Build Spark session with Snowflake Spark Connector ──
    spark = SparkSession.builder \
        .appName("Bank_Transactions_Load") \
        .master("local[*]") \
        .getOrCreate()

    gold_path = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"
    sf_options = _get_sf_options()

    # ==================================================================
    # PHASE 1: DIMENSIONS — Atomic Swap (Zero-Downtime Refresh)
    # ==================================================================
    print("\n" + "=" * 60)
    print("[Load] PHASE 1: Loading Dimensions (Atomic Swap)")
    print("=" * 60)

    # ── dim_account ──
    dim_account = spark.read.parquet(gold_path + "dim_account/")
    _atomic_swap_dimension(spark, dim_account, "DIM_ACCOUNT", sf_options)

    # ── dim_type ──
    dim_type = spark.read.parquet(gold_path + "dim_type/")
    _atomic_swap_dimension(spark, dim_type, "DIM_TYPE", sf_options)

    # ── dim_time ──
    dim_time = spark.read.parquet(gold_path + "dim_time/")
    _atomic_swap_dimension(spark, dim_time, "DIM_TIME", sf_options)

    # ==================================================================
    # PHASE 2: FACT TABLE — Idempotent Incremental Append
    # ==================================================================
    print("\n" + "=" * 60)
    print("[Load] PHASE 2: Loading Fact Table (Incremental Append)")
    print("=" * 60)

    # Ensure the target table exists before querying the high-water mark
    _ensure_fact_table_exists()

    # Read the high-water mark (max ingestion_timestamp already in Snowflake)
    high_water_mark = _get_high_water_mark(sf_options)

    # Read Gold fact data from HDFS
    fact_df = spark.read.parquet(gold_path + "fact_transactions/")

    # ── Filter to only net-new rows (after the high-water mark) ──
    if high_water_mark:
        fact_df = fact_df.filter(
            col("ingestion_timestamp") > high_water_mark
        )
        new_count = fact_df.count()
        if new_count == 0:
            print("[Load] No new fact rows to load. Skipping.")
            spark.stop()
            return
        print(f"[Load] {new_count:,} new rows to append.")
    else:
        print(f"[Load] Initial load: {fact_df.count():,} rows.")

    # ── Append net-new rows to Snowflake ──
    # mode("append") adds rows without touching existing data.
    # The high-water mark filter guarantees idempotency on retries.

    (
        fact_df.write.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", "FACT_TRANSACTIONS")
        .mode("append")
        .save()
    )

    print("[Load] ✅ All Gold data loaded into Snowflake successfully.")
    spark.stop()


if __name__ == "__main__":
    load_gold_to_snowflake()
