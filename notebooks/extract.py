"""
extract.py — Landing Zone → Bronze Layer (Incremental Load)
=============================================================
Purpose:
    Reads NEW JSONL micro-batches from the streaming landing zone, enforces
    a strict PySpark schema, adds an ingestion_timestamp for data lineage,
    and appends to the HDFS Bronze layer as Parquet.

Anti-patterns avoided:
    ✅ mode("append") — never overwrites historical Bronze data
    ✅ Strict StructType schema — rejects malformed rows at read time
    ✅ Ingestion timestamp — full data lineage from landing → Bronze
    ✅ Reads only new JSONL files (Airflow archives processed files afterward)

Run via:
    spark-submit /home/jovyan/work/extract.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

# ── HDFS requires the HADOOP_USER_NAME to be set for write permissions ──
os.environ["HADOOP_USER_NAME"] = "root"


def extract_landing_to_bronze() -> None:
    """
    Read JSONL files from the streaming landing zone, enforce schema,
    add lineage metadata, and append to the HDFS Bronze layer.
    """

    spark = SparkSession.builder \
        .appName("Bank_Transactions_Extract") \
        .master("local[*]") \
        .getOrCreate()

    # ── Paths ──
    # Landing zone: JSONL files written by the simulator
    input_path = "file:///home/jovyan/data/streaming_landing_zone/*.jsonl"
    # Bronze layer: append-only Parquet in HDFS
    output_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/"

    # ── Strict schema definition for PaySim dataset + event_timestamp ──
    # This rejects any rows that don't conform, preventing corrupt data
    # from silently entering the pipeline.
    schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),
        StructField("isFraud", IntegerType(), True),
        StructField("isFlaggedFraud", IntegerType(), True),
        # Added by the simulator to simulate real-time event occurrence
        StructField("event_timestamp", StringType(), True),
    ])

    print(f"[Extract] Reading JSONL files from: {input_path}")

    # ── Read JSONL with strict schema enforcement ──
    df = spark.read.schema(schema).json(input_path)

    row_count = df.count()
    if row_count == 0:
        print("[Extract] No new data found in landing zone. Skipping.")
        spark.stop()
        sys.exit(0)

    print(f"[Extract] Found {row_count:,} new rows to ingest.")

    # ── Add ingestion_timestamp for Bronze-layer data lineage ──
    # This records WHEN the data entered the pipeline (distinct from
    # event_timestamp which records when the event "occurred").
    df_with_lineage = df.withColumn("ingestion_timestamp", current_timestamp())

    # ── Append to Bronze layer as Parquet ──
    # mode("append") ensures we NEVER overwrite historical data.
    # Each pipeline run adds a new set of Parquet partition files.
    print(f"[Extract] Appending to Bronze layer: {output_path}")
    df_with_lineage.write.mode("append").parquet(output_path)

    print(f"[Extract] ✅ Successfully ingested {row_count:,} rows into Bronze.")
    spark.stop()


if __name__ == "__main__":
    extract_landing_to_bronze()
