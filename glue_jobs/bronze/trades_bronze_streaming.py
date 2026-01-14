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
TOPIC = "crypto.trades.raw"

BRONZE_PATH = "s3://crypto-datalake-prasanth/bronze/kafka_trades/"
CHECKPOINT_PATH = "s3://crypto-datalake-prasanth/checkpoints/trades_v3/"  # CHANGED: v3 for fresh start

# ------------------------------------------------------------------
# Trades schema (matches your sample messages)
# ------------------------------------------------------------------
trades_schema = StructType(
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
                            "trades",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("product_id", StringType(), True),
                                        StructField("trade_id", StringType(), True),
                                        StructField("price", StringType(), True),
                                        StructField("size", StringType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("side", StringType(), True),
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
    .select(F.from_json(F.col("json_value"), trades_schema).alias("data"))
    .select("data.*")
)

# ------------------------------------------------------------------
# Flatten minimal fields for bronze + partitions
# ------------------------------------------------------------------
flattened_df = (
    parsed_df.select(
        "channel",
        "timestamp",
        "ingest_ts",
        "source",
        F.explode_outer("events").alias("ev"),
    )
    .select(
        "channel",
        "timestamp",
        "ingest_ts",
        "source",
        F.col("ev.type").alias("event_type"),
        F.explode_outer("ev.trades").alias("trade"),
    )
    .select(
        "channel",
        "timestamp",
        "ingest_ts",
        "source",
        "event_type",
        F.col("trade.product_id").alias("product_id"),
        F.col("trade.trade_id").alias("trade_id"),
        F.col("trade.price").cast("double").alias("price"),
        F.col("trade.size").cast("double").alias("size"),
        F.col("trade.time").alias("trade_time"),
        F.col("trade.side").alias("side"),
    )
    .withColumn("event_date", F.to_date("timestamp"))
    .withColumn("event_hour", F.hour("timestamp"))
)

# ------------------------------------------------------------------
# Write to S3 bronze as Parquet
# ------------------------------------------------------------------
query = (
    flattened_df.writeStream.outputMode("append")
    .format("parquet")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("event_date")
    .start()
)

query.awaitTermination()

job.commit()
