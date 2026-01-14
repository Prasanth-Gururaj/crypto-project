# feature_engineering_record_based.py
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Paths
TRADES_PATH = "s3://crypto-datalake-prasanth/silver/trades/"
TICKER_PATH = "s3://crypto-datalake-prasanth/silver/ticker/"
FEATURES_PATH = "s3://crypto-datalake-prasanth/gold/features/"

print("="*60)
print("FEATURE ENGINEERING - RECORD-BASED (NO AGGREGATION)")
print("="*60)

# ========== 1. READ DATA ==========
print("\n1. Reading trades data...")
trades_df = spark.read.parquet(TRADES_PATH)
trades_count = trades_df.count()
print(f"Total trades: {trades_count}")

print("\n2. Reading ticker data...")
ticker_df = spark.read.parquet(TICKER_PATH)
ticker_count = ticker_df.count()
print(f"Total ticker records: {ticker_count}")

# Convert timestamps
trades_df = trades_df.withColumn("event_ts_dt", F.to_timestamp("event_ts"))
ticker_df = ticker_df.withColumn("event_ts_dt", F.to_timestamp("event_ts"))

# ========== 2. CREATE ROLLING WINDOW FEATURES ==========
print("\n3. Creating rolling window features on trades...")

# Define windows based on number of rows (not time!)
window_spec = Window.partitionBy("product_id").orderBy("event_ts_dt", "trade_id")

# Rolling windows of different sizes
window_10 = window_spec.rowsBetween(-9, 0)    # Last 10 trades
window_50 = window_spec.rowsBetween(-49, 0)   # Last 50 trades
window_100 = window_spec.rowsBetween(-99, 0)  # Last 100 trades
window_500 = window_spec.rowsBetween(-499, 0) # Last 500 trades

features_df = (
    trades_df
    
    # ===== PRICE FEATURES =====
    # Moving averages
    .withColumn("ma_10_trades", F.avg("price").over(window_10))
    .withColumn("ma_50_trades", F.avg("price").over(window_50))
    .withColumn("ma_100_trades", F.avg("price").over(window_100))
    .withColumn("ma_500_trades", F.avg("price").over(window_500))
    
    # Min/Max prices
    .withColumn("min_10_trades", F.min("price").over(window_10))
    .withColumn("max_10_trades", F.max("price").over(window_10))
    .withColumn("min_50_trades", F.min("price").over(window_50))
    .withColumn("max_50_trades", F.max("price").over(window_50))
    .withColumn("min_100_trades", F.min("price").over(window_100))
    .withColumn("max_100_trades", F.max("price").over(window_100))
    
    # Price range (volatility indicator)
    .withColumn("price_range_10", F.col("max_10_trades") - F.col("min_10_trades"))
    .withColumn("price_range_50", F.col("max_50_trades") - F.col("min_50_trades"))
    .withColumn("price_range_100", F.col("max_100_trades") - F.col("min_100_trades"))
    
    # Volatility (standard deviation)
    .withColumn("volatility_10", F.stddev("price").over(window_10))
    .withColumn("volatility_50", F.stddev("price").over(window_50))
    .withColumn("volatility_100", F.stddev("price").over(window_100))
    
    # Price changes from previous trades
    .withColumn("price_change_1", F.col("price") - F.lag("price", 1).over(window_spec))
    .withColumn("price_change_5", F.col("price") - F.lag("price", 5).over(window_spec))
    .withColumn("price_change_10", F.col("price") - F.lag("price", 10).over(window_spec))
    .withColumn("price_change_50", F.col("price") - F.lag("price", 50).over(window_spec))
    
    # Percentage returns
    .withColumn("return_1",
                F.when(F.lag("price", 1).over(window_spec).isNotNull(),
                       ((F.col("price") - F.lag("price", 1).over(window_spec)) / 
                        F.lag("price", 1).over(window_spec)) * 100).otherwise(0))
    .withColumn("return_10",
                F.when(F.lag("price", 10).over(window_spec).isNotNull(),
                       ((F.col("price") - F.lag("price", 10).over(window_spec)) / 
                        F.lag("price", 10).over(window_spec)) * 100).otherwise(0))
    .withColumn("return_50",
                F.when(F.lag("price", 50).over(window_spec).isNotNull(),
                       ((F.col("price") - F.lag("price", 50).over(window_spec)) / 
                        F.lag("price", 50).over(window_spec)) * 100).otherwise(0))
    .withColumn("return_100",
                F.when(F.lag("price", 100).over(window_spec).isNotNull(),
                       ((F.col("price") - F.lag("price", 100).over(window_spec)) / 
                        F.lag("price", 100).over(window_spec)) * 100).otherwise(0))
    
    # Distance from moving averages (trend indicators)
    .withColumn("price_vs_ma_10", 
                F.when(F.col("ma_10_trades").isNotNull() & (F.col("ma_10_trades") > 0),
                       (F.col("price") - F.col("ma_10_trades")) / F.col("ma_10_trades") * 100)
                .otherwise(0))
    .withColumn("price_vs_ma_50",
                F.when(F.col("ma_50_trades").isNotNull() & (F.col("ma_50_trades") > 0),
                       (F.col("price") - F.col("ma_50_trades")) / F.col("ma_50_trades") * 100)
                .otherwise(0))
    .withColumn("price_vs_ma_100",
                F.when(F.col("ma_100_trades").isNotNull() & (F.col("ma_100_trades") > 0),
                       (F.col("price") - F.col("ma_100_trades")) / F.col("ma_100_trades") * 100)
                .otherwise(0))
    
    # ===== VOLUME FEATURES =====
    # Rolling volume sums
    .withColumn("volume_10", F.sum("size").over(window_10))
    .withColumn("volume_50", F.sum("size").over(window_50))
    .withColumn("volume_100", F.sum("size").over(window_100))
    
    # Volume moving averages
    .withColumn("volume_ma_10", F.avg("size").over(window_10))
    .withColumn("volume_ma_50", F.avg("size").over(window_50))
    .withColumn("volume_ma_100", F.avg("size").over(window_100))
    
    # Current volume vs average
    .withColumn("volume_ratio_10",
                F.when(F.col("volume_ma_10") > 0,
                       F.col("size") / F.col("volume_ma_10")).otherwise(0))
    .withColumn("volume_ratio_50",
                F.when(F.col("volume_ma_50") > 0,
                       F.col("size") / F.col("volume_ma_50")).otherwise(0))
    
    # ===== BUY/SELL PRESSURE =====
    # Count buy vs sell in rolling windows
    .withColumn("buy_count_50",
                F.sum(F.when(F.col("side") == "BUY", 1).otherwise(0)).over(window_50))
    .withColumn("sell_count_50",
                F.sum(F.when(F.col("side") == "SELL", 1).otherwise(0)).over(window_50))
    .withColumn("buy_count_100",
                F.sum(F.when(F.col("side") == "BUY", 1).otherwise(0)).over(window_100))
    .withColumn("sell_count_100",
                F.sum(F.when(F.col("side") == "SELL", 1).otherwise(0)).over(window_100))
    
    # Buy/Sell ratio
    .withColumn("buy_sell_ratio_50",
                F.when(F.col("sell_count_50") > 0,
                       F.col("buy_count_50") / F.col("sell_count_50")).otherwise(0))
    .withColumn("buy_sell_ratio_100",
                F.when(F.col("sell_count_100") > 0,
                       F.col("buy_count_100") / F.col("sell_count_100")).otherwise(0))
    
    # Buy vs Sell volume
    .withColumn("buy_volume_50",
                F.sum(F.when(F.col("side") == "BUY", F.col("size")).otherwise(0)).over(window_50))
    .withColumn("sell_volume_50",
                F.sum(F.when(F.col("side") == "SELL", F.col("size")).otherwise(0)).over(window_50))
    .withColumn("buy_volume_100",
                F.sum(F.when(F.col("side") == "BUY", F.col("size")).otherwise(0)).over(window_100))
    .withColumn("sell_volume_100",
                F.sum(F.when(F.col("side") == "SELL", F.col("size")).otherwise(0)).over(window_100))
    
    # Buy volume as percentage of total
    .withColumn("buy_volume_ratio_50",
                F.when(F.col("volume_50") > 0,
                       F.col("buy_volume_50") / F.col("volume_50")).otherwise(0))
    .withColumn("buy_volume_ratio_100",
                F.when(F.col("volume_100") > 0,
                       F.col("buy_volume_100") / F.col("volume_100")).otherwise(0))
    
    # ===== TIME FEATURES =====
    .withColumn("hour_of_day", F.hour("event_ts_dt"))
    .withColumn("day_of_week", F.dayofweek("event_ts_dt"))
    .withColumn("is_weekend", F.when(F.dayofweek("event_ts_dt").isin([1, 7]), 1).otherwise(0))
    
    # ===== CURRENT TRADE FEATURES =====
    .withColumn("is_buy", F.when(F.col("side") == "BUY", 1).otherwise(0))
)

print("✓ Rolling window features created")

# ========== 3. ADD TICKER INFORMATION ==========
print("\n4. Adding ticker information (bid-ask spread)...")

# Match trades with ticker by minute
trades_with_minute = features_df.withColumn(
    "minute_ts", F.date_trunc("minute", "event_ts_dt")
)

ticker_with_minute = ticker_df.withColumn(
    "minute_ts", F.date_trunc("minute", "event_ts_dt")
)

# Get last ticker per minute per product
ticker_agg = (
    ticker_with_minute
    .withColumn("row_num", F.row_number().over(
        Window.partitionBy("product_id", "minute_ts").orderBy(F.desc("event_ts_dt"))
    ))
    .filter(F.col("row_num") == 1)
    .select(
        "product_id",
        "minute_ts",
        F.col("best_bid"),
        F.col("best_ask")
    )
)

# Join with trades
features_df = (
    trades_with_minute
    .join(ticker_agg, on=["product_id", "minute_ts"], how="left")
    .drop("minute_ts")
)

# Calculate spread features
features_df = (
    features_df
    .withColumn("bid_ask_spread", 
                F.when(F.col("best_ask").isNotNull() & F.col("best_bid").isNotNull(),
                       F.col("best_ask") - F.col("best_bid")).otherwise(0))
    .withColumn("spread_pct",
                F.when((F.col("best_bid").isNotNull()) & (F.col("best_bid") > 0),
                       ((F.col("best_ask") - F.col("best_bid")) / F.col("best_bid")) * 100)
                .otherwise(0))
    .withColumn("price_vs_bid",
                F.when(F.col("best_bid").isNotNull() & (F.col("best_bid") > 0),
                       (F.col("price") - F.col("best_bid")) / F.col("best_bid") * 100)
                .otherwise(0))
    .withColumn("price_vs_ask",
                F.when(F.col("best_ask").isNotNull() & (F.col("best_ask") > 0),
                       (F.col("price") - F.col("best_ask")) / F.col("best_ask") * 100)
                .otherwise(0))
)

print("✓ Ticker features added")

# ========== 4. CREATE TARGET VARIABLE ==========
print("\n5. Creating target variable (future price)...")

# Target: Price change after next 10 and 50 trades
features_df = (
    features_df
    .withColumn("future_price_10",
                F.lead("price", 10).over(window_spec))
    .withColumn("future_price_50",
                F.lead("price", 50).over(window_spec))
    
    # Target as percentage change (for regression)
    .withColumn("target_return_10",
                F.when(F.col("future_price_10").isNotNull(),
                       ((F.col("future_price_10") - F.col("price")) / F.col("price")) * 100)
                .otherwise(None))
    .withColumn("target_return_50",
                F.when(F.col("future_price_50").isNotNull(),
                       ((F.col("future_price_50") - F.col("price")) / F.col("price")) * 100)
                .otherwise(None))
    
    # Binary target: will price go up or down? (for classification)
    .withColumn("target_direction_10",
                F.when(F.col("target_return_10") > 0, 1)
                .when(F.col("target_return_10") < 0, 0)
                .otherwise(None))
    .withColumn("target_direction_50",
                F.when(F.col("target_return_50") > 0, 1)
                .when(F.col("target_return_50") < 0, 0)
                .otherwise(None))
)

print("✓ Target variables created")

# ========== 5. FILTER AND CLEAN ==========
print("\n6. Filtering out rows with insufficient history or no target...")

initial_count = features_df.count()
print(f"Records before filtering: {initial_count}")

# Remove rows without sufficient lookback (first 500 per product)
# Remove rows without target (last 50 per product)
features_df = features_df.filter(
    F.col("ma_500_trades").isNotNull() & 
    F.col("target_return_10").isNotNull()
)

final_count = features_df.count()
print(f"Records after filtering: {final_count}")
print(f"Removed: {initial_count - final_count} records")

# ========== 6. WRITE TO GOLD LAYER ==========
print("\n7. Writing features to gold layer...")

# Write to gold layer - partitioned by product and date
(
    features_df
    .write
    .mode("overwrite")
    .partitionBy("product_id", "event_date")
    .parquet(FEATURES_PATH)
)

print(f"\n{'='*60}")
print("✅ FEATURE ENGINEERING COMPLETED!")
print(f"{'='*60}")
print(f"Input trades: {trades_count}")
print(f"Input ticker: {ticker_count}")
print(f"Output features: {final_count}")
print(f"Output path: {FEATURES_PATH}")
print(f"{'='*60}")

job.commit()
