# Databricks notebook source
MY_ID = "yourname"

CATALOG = "workspace"
SCHEMA = f"capstone_{MY_ID}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

print("SCHEMA =", f"{CATALOG}.{SCHEMA}")
print("VOL =", VOL)
