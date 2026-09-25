# Databricks notebook source
MY_ID = "yourname"
CATALOG = "workspace"
SCHEMA = f"capstone_{MY_ID}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
DB = f"{CATALOG}.{SCHEMA}"

export_path = f"{VOL}/export/gold_tower_hour"

(
    spark.table(f"{DB}.gold_tower_hour")
         .coalesce(1)
         .write.mode("overwrite")
         .option("header",True)
         .csv(export_path)
)

print("Export:",export_path)
