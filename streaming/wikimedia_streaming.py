from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

KAFKA_TOPIC = "wikimedia-events"
KAFKA_BOOTSTRAP = "kafka-server:9092"

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
    .appName("WikimediaStructuredStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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
    col("id"),
    col("type"),
    col("title"),
    col("user"),
    col("bot"),
    col("wiki"),
    col("server_name"),
    col("timestamp"),
    to_timestamp(col("timestamp")).alias("event_time")
)

query = clean_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
