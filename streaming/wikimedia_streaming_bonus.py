from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

KAFKA_TOPIC = "wikimedia-events"
KAFKA_BOOTSTRAP = "kafka-server:9092"

STATIC_WIKI_REF_PATH = "hdfs://localhost:9000/final_project/static/wiki_reference.csv"

OUTPUT_PATH_ENRICHED = "hdfs://localhost:9000/final_project/wikimedia/event_counts_enriched"
CHECKPOINT_PATH_ENRICHED = "hdfs://localhost:9000/final_project/wikimedia/checkpoints/event_counts_enriched"

schema = StructType([
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", LongType(), True),
    StructField("title", StringType(), True),
    StructField("title_url", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("server_url", StringType(), True),
    StructField("server_name", StringType(), True),
    StructField("server_script_path", StringType(), True),
    StructField("wiki", StringType(), True)
])

spark = SparkSession.builder \
    .appName("WikimediaStructuredStreamingBonusV2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

wiki_ref_df = spark.read.option("header", True).csv(STATIC_WIKI_REF_PATH)

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")

clean_df = parsed_df.select(
    col("wiki"),
    col("type"),
    col("bot"),
    from_unixtime(col("timestamp")).cast("timestamp").alias("event_time")
).filter(
    col("event_time").isNotNull() &
    col("wiki").isNotNull() &
    col("type").isNotNull() &
    col("bot").isNotNull()
)

enriched_events_df = clean_df.join(
    wiki_ref_df,
    on="wiki",
    how="left"
)

agg_enriched_df = enriched_events_df.withWatermark("event_time", "2 minutes").groupBy(
    window(col("event_time"), "1 minute"),
    col("wiki"),
    col("wiki_group"),
    col("language_family"),
    col("region"),
    col("type"),
    col("bot")
).count().select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("wiki"),
    col("wiki_group"),
    col("language_family"),
    col("region"),
    col("type"),
    col("bot"),
    col("count")
)

query = agg_enriched_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", OUTPUT_PATH_ENRICHED) \
    .option("checkpointLocation", CHECKPOINT_PATH_ENRICHED) \
    .start()

query.awaitTermination()
