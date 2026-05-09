"""
load.py — Gold Layer → Snowflake (Atomic Swap + Idempotent Append)
====================================================================

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


def _upsert_dimension(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    pk_column: str,
    sf_options: dict,
) -> None:
    """
    MERGE-based upsert for dimension tables.

    Writes the incoming DataFrame to a staging table, then executes a
    MERGE INTO ... WHEN NOT MATCHED THEN INSERT to append only genuinely
    new dimension rows.  This preserves historical data across incremental
    micro-batch loads.
    """
    stage_table = f"{table_name}_STAGE"

    print(f"[Load] Upsert: Writing to staging table {stage_table}...")

    # Step 1: Write the current batch to the staging table
    (
        df.write.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", stage_table)
        .mode("overwrite")
        .save()
    )

    # Step 2: MERGE new rows into the production table
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
        # Ensure the production table exists (mirrors staging schema)
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {stage_table}"
        )

        # Dynamically fetch column names from the staging table
        cursor.execute(f"SHOW COLUMNS IN TABLE {stage_table}")
        columns = [row[2] for row in cursor.fetchall()]  # column_name is index 2

        cols_csv = ", ".join(columns)
        source_cols_csv = ", ".join(f"SOURCE.{c}" for c in columns)

        merge_sql = (
            f"MERGE INTO {table_name} TARGET "
            f"USING {stage_table} SOURCE "
            f"ON TARGET.{pk_column} = SOURCE.{pk_column} "
            f"WHEN NOT MATCHED THEN INSERT ({cols_csv}) "
            f"VALUES ({source_cols_csv})"
        )

        print(f"[Load] Upsert: Executing MERGE into {table_name}...")
        cursor.execute(merge_sql)
        print(f"[Load] Upsert: ✅ {table_name} merged successfully.")

        # Step 3: Drop the staging table
        cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")
        print(f"[Load] Upsert: Cleaned up {stage_table}.")
    except Exception as e:
        print(f"[Load] Upsert: ❌ MERGE failed for {table_name}: {e}")
        cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")
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

    gold_path = "hdfs://hadoop-namenode:9000/gold_layer/"
    sf_options = _get_sf_options()

    # ==================================================================
    # PHASE 1: DIMENSIONS — MERGE Upsert (Preserves Historical Data)
    # ==================================================================
    print("\n" + "=" * 60)
    print("[Load] PHASE 1: Loading Dimensions (MERGE Upsert)")
    print("=" * 60)

    # ── dim_account ──
    dim_account = spark.read.parquet(gold_path + "dim_account/")
    _upsert_dimension(spark, dim_account, "DIM_ACCOUNT", "ACCOUNT_ID", sf_options)

    # ── dim_type ──
    dim_type = spark.read.parquet(gold_path + "dim_type/")
    _upsert_dimension(spark, dim_type, "DIM_TYPE", "TYPE_ID", sf_options)

    # ── dim_time ──
    dim_time = spark.read.parquet(gold_path + "dim_time/")
    _upsert_dimension(spark, dim_time, "DIM_TIME", "TIME_ID", sf_options)

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
