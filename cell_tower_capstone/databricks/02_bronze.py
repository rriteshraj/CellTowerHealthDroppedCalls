# Databricks notebook source
import pyspark.sql.functions as F

MY_ID = "yourname"
CATALOG = "workspace"
SCHEMA = f"capstone_{MY_ID}"
DB = f"{CATALOG}.{SCHEMA}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

def add_metadata(df):
    raw_cols = [F.col(c).cast("string") for c in df.columns]
    return (
        df.select([F.col(c).cast("string").alias(c) for c in df.columns])
          .withColumn("_source_file", F.input_file_name())
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_row_hash", F.sha2(F.concat_ws("||", *raw_cols), 256))
    )

towers = add_metadata(
    spark.read.option("header",True).option("inferSchema",False).csv(f"{VOL}/raw/towers")
)
calls = add_metadata(
    spark.read.option("header",True).option("inferSchema",False).csv(f"{VOL}/raw/calls")
)

towers.write.mode("overwrite").format("delta").saveAsTable(f"{DB}.bronze_towers")
calls.write.mode("overwrite").format("delta").saveAsTable(f"{DB}.bronze_calls")

assert spark.table(f"{DB}.bronze_towers").count() == 120
assert spark.table(f"{DB}.bronze_calls").count() == 376200
