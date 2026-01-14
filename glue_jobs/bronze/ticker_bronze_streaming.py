import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ------------------------------------------------------------------
# Job / Glue setup
# ------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
KAFKA_BOOTSTRAP = "3.235.139.215:9092"  # EC2 public IP
TOPIC = "crypto.ticker.raw"

BRONZE_PATH = "s3://crypto-datalake-prasanth/bronze/kafka_ticker/"
CHECKPOINT_PATH = "s3://crypto-datalake-prasanth/checkpoints/ticker_v3/"  # CHANGED: v3 for fresh start

# ------------------------------------------------------------------
# Define a LIGHT schema for bronze (matches your messages)
# ------------------------------------------------------------------
ticker_schema = StructType(
    [
        StructField("channel", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("sequence_num", LongType(), True),
        StructField(
            "events",
            ArrayType(
                StructType(
                    [
                        StructField("type", StringType(), True),
                        StructField(
                            "tickers",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("type", StringType(), True),
                                        StructField("product_id", StringType(), True),
                                        StructField("price", StringType(), True),
                                        StructField("volume_24_h", StringType(), True),
                                        StructField("low_24_h", StringType(), True),
                                        StructField("high_24_h", StringType(), True),
                                        StructField("low_52_w", StringType(), True),
                                        StructField("high_52_w", StringType(), True),
                                        StructField("price_percent_chg_24_h", StringType(), True),
                                        StructField("best_bid", StringType(), True),
                                        StructField("best_bid_quantity", StringType(), True),
                                        StructField("best_ask", StringType(), True),
                                        StructField("best_ask_quantity", StringType(), True),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
        StructField("ingest_ts", StringType(), True),
        StructField("source", StringType(), True),
    ]
)

# ------------------------------------------------------------------
# Read from Kafka as stream
# ------------------------------------------------------------------
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")  # ADDED: Tolerate offset mismatches in dev
    .load()
)

# value is binary -> cast to string, parse JSON
parsed_df = (
    kafka_df.select(F.col("value").cast("string").alias("json_value"))
    .select(F.from_json(F.col("json_value"), ticker_schema).alias("data"))
    .select("data.*")
)

# ------------------------------------------------------------------
# Bronze write: keep it mostly raw, but add partition columns
# ------------------------------------------------------------------
bronze_df = (
    parsed_df.withColumn("event_date", F.to_date("timestamp"))
    .withColumn("event_hour", F.hour("timestamp"))
)

query = (
    bronze_df.writeStream.outputMode("append")
    .format("parquet")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("event_date")
    .start()
)

query.awaitTermination()

job.commit()
