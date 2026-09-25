# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window

MY_ID = "yourname"
CATALOG = "workspace"
SCHEMA = f"capstone_{MY_ID}"
DB = f"{CATALOG}.{SCHEMA}"

calls = spark.table(f"{DB}.silver_calls")
towers = spark.table(f"{DB}.silver_towers")

base = (
    calls
    .withColumn("call_date",F.to_date("start_time"))
    .withColumn("hour_of_day",F.hour("start_time"))
    .groupBy("tower_id","call_date","hour_of_day")
    .agg(
        F.count("*").alias("calls"),
        F.sum("is_dropped").alias("dropped_calls"),
        F.avg("duration_int").alias("avg_duration_seconds")
    )
)

events = (
    calls
    .withColumn("call_date",F.to_date("start_time"))
    .withColumn("hour_of_day",F.hour("start_time"))
)

starts = events.select(
    "tower_id","call_date","hour_of_day",
    F.col("start_time").alias("event_ts"),F.lit(1).alias("delta")
)
ends = events.select(
    "tower_id","call_date","hour_of_day",
    F.col("end_ts").alias("event_ts"),F.lit(-1).alias("delta")
)

event_points = (
    starts.unionByName(ends)
          .groupBy("tower_id","call_date","hour_of_day","event_ts")
          .agg(F.sum("delta").alias("delta"))
)

cw = (
    Window.partitionBy("tower_id","call_date","hour_of_day")
          .orderBy("event_ts")
          .rowsBetween(Window.unboundedPreceding,Window.currentRow)
)

peak = (
    event_points
    .withColumn("concurrent",F.sum("delta").over(cw))
    .groupBy("tower_id","call_date","hour_of_day")
    .agg(F.max("concurrent").alias("peak_concurrent"))
)

gold = (
    base.join(peak,["tower_id","call_date","hour_of_day"],"left")
        .join(towers,"tower_id","left")
        .withColumn("drop_rate",F.col("dropped_calls")/F.col("calls"))
        .withColumn("utilisation",F.col("peak_concurrent")/F.col("capacity_channels"))
        .select(
            "tower_id","site_name","city","district","capacity_channels",
            "call_date","hour_of_day","calls","dropped_calls","drop_rate",
            "peak_concurrent","utilisation","avg_duration_seconds"
        )
)

gold.write.mode("overwrite").format("delta").saveAsTable(f"{DB}.gold_tower_hour")

assert spark.table(f"{DB}.gold_tower_hour").count() == 43142
print("Gold rows:",spark.table(f"{DB}.gold_tower_hour").count())
spark.table(f"{DB}.gold_tower_hour").agg(
    F.sum("calls").alias("calls"),
    F.sum("dropped_calls").alias("drops")
).withColumn("drop_rate",F.col("drops")/F.col("calls")).show()
