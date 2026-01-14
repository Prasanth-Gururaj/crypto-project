# crypto-project
# Real-Time Cryptocurrency Data Pipeline

End-to-end streaming data pipeline for real-time cryptocurrency market data using AWS Glue, Apache Kafka, and PySpark. Implements medallion architecture (Bronze → Silver → Gold) for data processing and feature engineering.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Athena-orange)
![Kafka](https://img.shields.io/badge/Kafka-3.7-black)
![PySpark](https://img.shields.io/badge/PySpark-3.3-red)

---

## 📊 Project Overview

This pipeline ingests live cryptocurrency trading data from **Coinbase WebSocket API**, processes it through a **serverless ETL architecture**, and creates **analytics-ready datasets** with 70+ engineered features for machine learning and business intelligence.

**Key Metrics:**
- **Throughput**: 10+ messages/second sustained
- **Latency**: <10 minutes end-to-end (ingestion → analytics)
- **Data Volume**: 50,000+ records processed daily
- **Features**: 70+ ML-ready features engineered in Gold layer

---

## 🏗️ Architecture

### High-Level Overview

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Coinbase   │──────▶│   Kafka     │──────▶│ AWS Glue    │
│  WebSocket  │ Live  │   (EC2)     │ Stream│  Streaming  │
└─────────────┘ Data  └─────────────┘       └─────────────┘
                                                    │
                                                    ▼
                                            ┌──────────────┐
                                            │  S3 Bronze   │
                                            │   (Raw)      │
                                            └──────┬───────┘
                                                   │
                                                   ▼
                                            ┌──────────────┐
                                            │  AWS Glue    │
                                            │  Batch ETL   │
                                            └──────┬───────┘
                                                   │
                                                   ▼
                                            ┌──────────────┐
                                            │  S3 Silver   │
                                            │  (Cleaned)   │
                                            └──────┬───────┘
                                                   │
                                                   ▼
                                            ┌──────────────┐
                                            │  AWS Glue    │
                                            │  Features    │
                                            └──────┬───────┘
                                                   │
                                                   ▼
                                            ┌──────────────┐
                                            │  S3 Gold     │
                                            │  (ML Ready)  │
                                            └──────┬───────┘
                                                   │
                                                   ▼
                                            ┌──────────────┐
                                            │   Athena &   │
                                            │  QuickSight  │
                                            └──────────────┘
```

### Detailed Component Breakdown

#### 🔹 Ingestion Layer
- **Producer**: Python script running locally
- **WebSocket**: Coinbase Pro real-time feed
- **Products**: BTC-USD, ETH-USD
- **Channels**: Ticker (price updates) + Matches (executed trades)

#### 🔹 Messaging Layer
- **Kafka Broker**: EC2 t3.medium with Docker
- **Mode**: KRaft (no ZooKeeper dependency)
- **Topics**: `crypto.ticker.raw`, `crypto.trades.raw`
- **Retention**: 7 days

#### 🔹 Bronze Layer (Raw Data Lake)
- **Format**: Parquet with Snappy compression
- **Partitioning**: By `ingest_date` (YYYY-MM-DD)
- **Schema**: Preserves original JSON structure
- **Size**: ~15 MB/day per topic

#### 🔹 Silver Layer (Cleaned Data)
- **Transformations**: Type casting, deduplication, null handling
- **Partitioning**: By `product_id` and `event_date`
- **Deduplication**: By `trade_id` and `sequence`
- **Size**: ~12 MB/day per topic

#### 🔹 Gold Layer (Feature Store)
- **Features**: 70+ engineered features
  - Price: MAs, volatility, returns, momentum
  - Volume: Rolling sums, buy/sell ratios
  - Market: Bid-ask spread, price vs MA
  - Time: Hour, day, weekend flags
- **Targets**: Future price movement (10/50 trades ahead)
- **Size**: ~500 KB/day

#### 🔹 Analytics Layer
- **Athena**: Serverless SQL queries on S3
- **QuickSight**: BI dashboards and visualizations
- **Cost**: Pay-per-query (data scanned)
```
---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Source** | Coinbase WebSocket API | Real-time market data (ticker, trades) |
| **Producer** | Python, websocket-client, kafka-python | Stream data from Coinbase to Kafka |
| **Message Queue** | Apache Kafka 3.7 (KRaft mode) | Decouple ingestion from processing |
| **Stream Processing** | AWS Glue Streaming (PySpark) | Kafka consumer → Bronze layer |
| **Batch Processing** | AWS Glue Batch (PySpark) | Silver/Gold transformations |
| **Storage** | AWS S3 (Parquet) | Data lake with medallion architecture |
| **Catalog** | AWS Glue Data Catalog | Metadata management |
| **Analytics** | AWS Athena, QuickSight | SQL queries and dashboards |
| **Orchestration** | AWS Glue Triggers | Job scheduling and dependencies |
| **Monitoring** | CloudWatch Logs & Metrics | Pipeline observability |

---

## 🎯 Features

### Data Pipeline
- ✅ **Real-time ingestion** from Coinbase WebSocket (ticker + trades channels)
- ✅ **Kafka buffering** on EC2 with KRaft mode (no ZooKeeper)
- ✅ **Serverless streaming** with AWS Glue (Kafka consumer)
- ✅ **Medallion architecture** (Bronze → Silver → Gold)
- ✅ **Incremental processing** with watermark-based deduplication
- ✅ **Parquet compression** (5x size reduction vs JSON)
- ✅ **Partition pruning** (95% reduction in query scan costs)

### Data Layers

**Bronze Layer (Raw Ingestion)**
- Minimal transformation, preserves original JSON structure
- Parquet format with Snappy compression
- Partitioned by `ingest_date`
- Retention: All historical data

**Silver Layer (Cleaned & Typed)**
- Type casting (string → double, timestamp conversion)
- Deduplication by `trade_id` / `sequence`
- Null handling and validation
- Partitioned by `product_id` and `event_date`
- Schema enforcement

**Gold Layer (Analytics & ML Features)**
- **70+ engineered features**:
  - Rolling price statistics (MA 10/50/100/500 trades)
  - Volatility metrics (standard deviation)
  - Momentum indicators (returns over 1/10/50/100 trades)
  - Volume analysis (rolling sums, buy/sell ratios)
  - Buy/sell pressure (volume-weighted ratios)
  - Bid-ask spread metrics
  - Time-based features (hour, day, weekend flag)
- **Target variables** for ML (future price movement 10/50 trades ahead)
- Filters rows with insufficient lookback history
- Optimized for model training and BI dashboards
