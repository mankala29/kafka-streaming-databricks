import dlt
from pyspark.sql.functions import *

@dlt.table
def bronze():
    return spark.readStream.format("delta").load("/mnt/bronze/user_events")

@dlt.table
def silver_cleaned():
    return (
        dlt.read_stream("bronze")
        .withColumn("event_time", (col("ts")/1000).cast("timestamp"))
    )

@dlt.table
def gold_user_actions():
    return (
        dlt.read("silver_cleaned")
        .groupBy("user_id", "action")
        .count()
        .withColumnRenamed("count", "action_count")
    )