import sys, json, boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_PATH = "s3://crypto-datalake-prasanth/bronze/kafka_ticker/"
SILVER_PATH = "s3://crypto-datalake-prasanth/silver/ticker/"

WATERMARK_BUCKET = "crypto-datalake-prasanth"
WATERMARK_KEY = "watermarks/ticker_ingest_ts.json"

s3 = boto3.client("s3")

def read_watermark():
    try:
        obj = s3.get_object(Bucket=WATERMARK_BUCKET, Key=WATERMARK_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data.get("last_ingest_ts")
    except s3.exceptions.NoSuchKey:
        return None

def write_watermark(ts):
    body = json.dumps({"last_ingest_ts": ts})
    s3.put_object(Bucket=WATERMARK_BUCKET, Key=WATERMARK_KEY, Body=body)

# 1) Read bronze
bronze_df = spark.read.parquet(BRONZE_PATH)

# 2) Filter to only new records by ingest_ts
last_ingest_ts = read_watermark()
if last_ingest_ts:
    incr_df = bronze_df.filter(F.col("ingest_ts") > last_ingest_ts)
else:
    incr_df = bronze_df

if incr_df.rdd.isEmpty():
    job.commit()
    sys.exit(0)

# 3) Flatten + select
flattened_df = (
    incr_df
    .withColumn("event", F.explode("events"))
    .withColumn("ticker", F.explode("event.tickers"))
)

silver_df = (
    flattened_df.select(
        F.col("channel"),
        F.col("timestamp").alias("event_ts"),
        F.col("ingest_ts"),
        F.col("source"),
        F.col("event_hour"),
        F.col("event_date"),
        F.col("ticker.product_id").alias("product_id"),
        F.col("ticker.price").cast("double").alias("price"),
        F.col("ticker.volume_24_h").cast("double").alias("volume_24h"),
        F.col("ticker.low_24_h").cast("double").alias("low_24h"),
        F.col("ticker.high_24_h").cast("double").alias("high_24h"),
        F.col("ticker.low_52_w").cast("double").alias("low_52w"),
        F.col("ticker.high_52_w").cast("double").alias("high_52w"),
        F.col("ticker.price_percent_chg_24_h")
            .cast("double")
            .alias("price_change_24h"),
        F.col("ticker.best_bid").cast("double").alias("best_bid"),
        F.col("ticker.best_bid_quantity").cast("double").alias("best_bid_qty"),
        F.col("ticker.best_ask").cast("double").alias("best_ask"),
        F.col("ticker.best_ask_quantity").cast("double").alias("best_ask_qty"),
    )
)

# 4) Append to silver
(
    silver_df.write.mode("append")
    .partitionBy("event_date", "product_id")
    .parquet(SILVER_PATH)
)

# 5) Update watermark
max_ingest = incr_df.agg(F.max("ingest_ts").alias("max_ts")).collect()[0]["max_ts"]
write_watermark(max_ingest)

job.commit()
