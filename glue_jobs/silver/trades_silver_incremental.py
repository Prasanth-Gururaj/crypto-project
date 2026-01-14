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

BRONZE_PATH = "s3://crypto-datalake-prasanth/bronze/kafka_trades/"
SILVER_PATH = "s3://crypto-datalake-prasanth/silver/trades/"

WATERMARK_BUCKET = "crypto-datalake-prasanth"
WATERMARK_KEY = "watermarks/trades_ingest_ts.json"

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

# 2) Filter to only new records
last_ingest_ts = read_watermark()
if last_ingest_ts:
    incr_df = bronze_df.filter(F.col("ingest_ts") > last_ingest_ts)
else:
    incr_df = bronze_df

if incr_df.rdd.isEmpty():
    job.commit()
    sys.exit(0)

# 3) Select / cast
silver_df = (
    incr_df.select(
        F.col("channel"),
        F.col("timestamp").alias("event_ts"),
        F.col("ingest_ts"),
        F.col("source"),
        F.col("event_type"),
        F.col("event_hour"),
        F.col("event_date"),
        F.col("product_id"),
        F.col("trade_id"),
        F.col("price").cast("double").alias("price"),
        F.col("size").cast("double").alias("size"),
        F.col("trade_time"),
        F.col("side"),
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
