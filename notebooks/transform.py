"""
transform.py — Bronze → Gold (Star Schema Transformation)
============================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    to_timestamp,
    year,
    month,
    dayofmonth,
    dayofweek,
    when,
)

# ── HDFS requires the HADOOP_USER_NAME to be set for write permissions ──
os.environ["HADOOP_USER_NAME"] = "root"


def transform_bronze_to_gold() -> None:
    """
    Read Bronze Parquet, build Star Schema dimensions and fact table,
    and write to the Gold layer in HDFS.
    """

    spark = SparkSession.builder \
        .appName("Bank_Transactions_Transform") \
        .master("local[*]") \
        .getOrCreate()

    # ── Paths ──
    bronze_path = "hdfs://hadoop-namenode:9000/bronze_layer"
    gold_path = "hdfs://hadoop-namenode:9000/gold_layer/"

    print(f"[Transform] Reading Bronze layer from: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    total_rows = df.count()
    print(f"[Transform] Bronze contains {total_rows:,} rows.")

    # =====================================================================
    # DIMENSION: dim_time — Static Date Dimension
    # =====================================================================
    # Derived from event_timestamp. Contains advanced calendar attributes
    # for analytical queries (weekend analysis, monthly trends, etc.).
    print("[Transform] Building dim_time...")

    df_with_ts = df.withColumn(
        "event_ts", to_timestamp(col("event_timestamp"))
    )

    dim_time = (
        df_with_ts
        .select(
            year("event_ts").alias("year"),
            month("event_ts").alias("month"),
            dayofmonth("event_ts").alias("day"),
            dayofweek("event_ts").alias("day_of_week"),
        )
        .distinct()
        .withColumn(
            # weekend_flag: 1=Sunday, 7=Saturday in Spark's dayofweek()
            "weekend_flag",
            when(
                (col("day_of_week") == 1) | (col("day_of_week") == 7), 1
            ).otherwise(0),
        )
        .withColumn("time_id", monotonically_increasing_id())
    )

    # =====================================================================
    # DIMENSION: dim_account — Role-Playing Dimension
    # =====================================================================
    # Combines all unique account names from both nameOrig (sender) and
    # nameDest (receiver). In the fact table, this dimension is referenced
    # twice via orig_account_id and dest_account_id (role-playing pattern).
    print("[Transform] Building dim_account...")

    accounts_orig = df.select(col("nameOrig").alias("account_name"))
    accounts_dest = df.select(col("nameDest").alias("account_name"))

    dim_account = (
        accounts_orig.union(accounts_dest)
        .distinct()
        .withColumn("account_id", monotonically_increasing_id())
    )

    # =====================================================================
    # DIMENSION: dim_type — Transaction Type Dimension
    # =====================================================================
    print("[Transform] Building dim_type...")

    dim_type = (
        df.select("type")
        .distinct()
        .withColumn("type_id", monotonically_increasing_id())
    )

    # =====================================================================
    # FACT TABLE: fact_transactions
    # =====================================================================
    # Joins to all dimensions to replace natural keys with surrogate keys.
    # Uses Role-Playing Dimension: dim_account is joined TWICE — once for
    # origin and once for destination — producing two FK columns.
    print("[Transform] Building fact_transactions...")

    # Alias dim_account for the two role-playing joins
    dim_account_orig = dim_account.alias("orig")
    dim_account_dest = dim_account.alias("dest")

    # Join to dim_type
    fact = df_with_ts.join(dim_type, on="type", how="left")

    # Join to dim_account for ORIGIN (role: sender)
    fact = fact.join(
        dim_account_orig,
        col("nameOrig") == col("orig.account_name"),
        how="left",
    ).withColumnRenamed("account_id", "orig_account_id").drop(
        col("orig.account_name")
    )

    # Join to dim_account for DESTINATION (role: receiver)
    fact = fact.join(
        dim_account_dest,
        col("nameDest") == col("dest.account_name"),
        how="left",
    ).withColumnRenamed("account_id", "dest_account_id").drop(
        col("dest.account_name")
    )

    # Join to dim_time
    fact = fact.join(
        dim_time,
        (year("event_ts") == dim_time["year"])
        & (month("event_ts") == dim_time["month"])
        & (dayofmonth("event_ts") == dim_time["day"]),
        how="left",
    )

    # Select final fact columns with surrogate keys
    fact_transactions = fact.select(
        monotonically_increasing_id().alias("transaction_id"),
        col("step"),
        col("type_id"),
        col("time_id"),
        col("amount"),
        col("orig_account_id"),
        col("oldbalanceOrg"),
        col("newbalanceOrig"),
        col("dest_account_id"),
        col("oldbalanceDest"),
        col("newbalanceDest"),
        col("isFraud"),
        col("isFlaggedFraud"),
        col("ingestion_timestamp"),
    )

    # =====================================================================
    # WRITE Gold Layer
    # =====================================================================
    # mode("overwrite") is correct for Gold: it's a derived layer rebuilt
    # entirely from Bronze. This makes the transformation idempotent —
    # re-running produces the exact same result without duplicates.
    print("[Transform] Writing Star Schema to Gold layer...")

    dim_time.write.mode("overwrite").parquet(gold_path + "dim_time/")
    dim_account.write.mode("overwrite").parquet(gold_path + "dim_account/")
    dim_type.write.mode("overwrite").parquet(gold_path + "dim_type/")
    fact_transactions.write.mode("overwrite").parquet(
        gold_path + "fact_transactions/"
    )

    print("[Transform] ✅ Star Schema written to Gold layer successfully.")
    print(f"  dim_time:           {dim_time.count():,} rows")
    print(f"  dim_account:        {dim_account.count():,} rows")
    print(f"  dim_type:           {dim_type.count():,} rows")
    print(f"  fact_transactions:  {fact_transactions.count():,} rows")

    spark.stop()


if __name__ == "__main__":
    transform_bronze_to_gold()
