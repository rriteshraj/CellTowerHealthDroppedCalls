# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import Window

MY_ID = "yourname"
CATALOG = "workspace"
SCHEMA = f"capstone_{MY_ID}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

CI = "element_at(array('Pune','Kochi','Indore','Jaipur','Nagpur','Surat'),cast(id%6 as int)+1)"
PK = "(9,10,11,12,17,18,19,20)"
MD = "(6,7,8,13,14,15,16,21)"

t = spark.range(120).selectExpr(
    "id n",
    "format_string('T%03d',id+1) tower_id",
    f"concat({CI},' Site ',format_string('%03d',id+1)) site_name",
    f"{CI} city",
    "format_string('D%d',cast(id%4 as int)+1) district",
    "if(id<80,10,2) capacity_channels",
    "date_add(date'2019-01-01',cast(id*11 as int)) commissioned_on"
)
t.drop("n").write.mode("overwrite").option("header",True).csv(f"{VOL}/raw/towers")

c = (
    t.withColumn("call_date", F.explode(F.expr("sequence(date'2025-06-01',date'2025-06-15')")))
     .withColumn("hour_of_day", F.explode(F.expr("sequence(0,23)")))
     .selectExpr(
         "n","tower_id","call_date","hour_of_day",
         "if(n<80,6,1) reps",
         f"case when hour_of_day in {PK} then 3 when hour_of_day in {MD} then 2 else 1 end w"
     )
     .selectExpr(
         "*","w*reps cnt",
         "if(n<80,element_at(array(360,180,72),w),element_at(array(20,10,6),w)) md"
     )
     .withColumn("k", F.explode(F.expr("sequence(1,cnt)")))
     .selectExpr(
         "*",
         "hour_of_day*3600+(k-1)*cast(3600/cnt as int) ss",
         "element_at(array(45,240,420),w)+(k%4)*element_at(array(15,120,120),w) dur",
         "concat_ws('-',tower_id,string(call_date),string(hour_of_day),string(k)) call_id"
     )
     .withColumn(
         "rb",
         F.row_number().over(
             Window.partitionBy("tower_id","w").orderBy("call_date","hour_of_day","k")
         )
     )
)

c = c.selectExpr(
    "*",
    "case when rb%md=0 then 'DROPPED' when rb%17=1 then 'BUSY' "
    "when rb%23=2 then 'NO_ANSWER' else 'NORMAL' end cause"
).withColumn("rn", F.row_number().over(Window.orderBy("call_id")))

raw = c.selectExpr(
    "rn","call_id",
    "format_string('SUB%05d',cast(pmod(k*37+hour_of_day*11+n*7,60000) as int)) subscriber_id",
    "if(rn%624=2,'T999',tower_id) tower_id",
    "cast(timestampadd(second,ss,cast(call_date as timestamp)) as string) start_ts",
    "case when rn%312=0 then 'NA' when rn%832=3 then '-1' else string(dur) end duration_seconds",
    "if(rn%416=1,concat(' ',lower(cause),' '),cause) end_cause",
    "if(k%2=0,'MT','MO') direction",
    "if(n%3=0,'5G','4G') technology"
)

(
    raw.unionByName(raw.filter("rn%208=5"))
       .drop("rn")
       .write.mode("overwrite")
       .option("header",True)
       .option("ignoreLeadingWhiteSpace",False)
       .option("ignoreTrailingWhiteSpace",False)
       .csv(f"{VOL}/raw/calls")
)

print("Towers:", spark.read.option("header",True).csv(f"{VOL}/raw/towers").count())
print("Calls:", spark.read.option("header",True).csv(f"{VOL}/raw/calls").count())
