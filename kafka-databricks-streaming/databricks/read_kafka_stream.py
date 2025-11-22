from pyspark.sql.functions import *

df_raw = (
  spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.0.104:9092")
      .option("subscribe", "user-events")
      .load()
)

df_parsed = df_raw.select(
    col("value").cast("string").alias("json")
).select(
    from_json("json", """
      event_id STRING,
      user_id INT,
      action STRING,
      ts BIGINT
    """).alias("data")
).select("data.*")

(df_parsed.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/bronze/check_user_events")
    .option("path", "/mnt/bronze/user_events")
    .trigger(availableNow=True)
    .start()
)