# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window

MY_ID = "yourname"
CATALOG = "workspace"
SCHEMA = f"capstone_{MY_ID}"
DB = f"{CATALOG}.{SCHEMA}"

towers = spark.table(f"{DB}.bronze_towers")
calls = spark.table(f"{DB}.bronze_calls")

silver_towers = towers.select(
    "tower_id","site_name","city","district",
    F.expr("try_cast(capacity_channels as int)").alias("capacity_channels"),
    F.expr("try_cast(commissioned_on as date)").alias("commissioned_on")
)

# Dedupe on call_id.
w = Window.partitionBy("call_id").orderBy(F.col("_ingested_at").asc())
deduped = (
    calls.withColumn("_rn",F.row_number().over(w))
         .filter("_rn = 1")
         .drop("_rn")
)

typed = (
    deduped
    .withColumn("duration_int",F.expr("try_cast(duration_seconds as int)"))
    .withColumn("start_time",F.expr("try_cast(start_ts as timestamp)"))
    .withColumn("end_cause_clean",F.upper(F.trim("end_cause")))
)

# Rejections required by the supplied brief.
r1 = typed.filter(F.col("duration_int").isNull()).select("call_id").withColumn(
    "reason",F.lit("duration_not_numeric")
)

r2 = typed.join(silver_towers.select("tower_id"),"tower_id","left_anti").select(
    "call_id"
).withColumn("reason",F.lit("unknown_tower"))

r3 = typed.filter(
    F.col("duration_int").isNotNull() &
    ((F.col("duration_int") < 0) | (F.col("duration_int") > 7200))
).select("call_id").withColumn("reason",F.lit("duration_out_of_range"))

rejects = r1.unionByName(r2).unionByName(r3)

valid = (
    typed.join(silver_towers.select("tower_id"),"tower_id","inner")
         .filter(F.col("duration_int").isNotNull())
         .filter((F.col("duration_int") >= 0) & (F.col("duration_int") <= 7200))
         .filter(F.col("start_time").isNotNull())
         .withColumn("end_ts",F.expr("start_time + INTERVAL 1 SECOND * duration_int"))
         .withColumn("is_dropped",(F.col("end_cause_clean")=="DROPPED").cast("int"))
         .select(
             "call_id","subscriber_id","tower_id","start_time","end_ts",
             "duration_int","end_cause_clean","direction","technology",
             "is_dropped","_source_file","_ingested_at","_row_hash"
         )
)

silver_towers.write.mode("overwrite").format("delta").saveAsTable(f"{DB}.silver_towers")
valid.write.mode("overwrite").format("delta").saveAsTable(f"{DB}.silver_calls")
rejects.write.mode("overwrite").format("delta").saveAsTable(f"{DB}.silver_rejects")

assert spark.table(f"{DB}.silver_calls").count() == 372150
