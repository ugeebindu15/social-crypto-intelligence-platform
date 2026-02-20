"""
Spark Streaming - Crypto Price Processor
-----------------------------------------
- Reads crypto prices from Kafka topic 'crypto_prices'
- Transforms and cleans into Silver layer
- Aggregates 1-minute OHLC windows into Gold layer
- Loads both into Snowflake

Run this script directly:
    python dags/spark_crypto_stream.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, first, last,
    count, round as spark_round, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, TimestampType
)
from snowflake.connector import connect
import json

# ── Snowflake Connection ───────────────────────────────────────────────────────
def get_snowflake_conn():
    return connect(
        account='pm48117.us-east-2.aws',
        user='HIMABINDUT9715',
        password='Pradhyunsai9715!',
        warehouse='AIRFLOW_WH',
        database='SOCIAL_ANALYTICS',
        schema='BRONZE'
    )

# ── Spark Session ──────────────────────────────────────────────────────────────
print("🚀 Starting Spark Session...")
spark = SparkSession.builder \
    .appName("CryptoPriceStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session started")

# ── Schema for Kafka messages ──────────────────────────────────────────────────
# Matches exactly what our producer sends
schema = StructType([
    StructField("symbol",    StringType(),  True),
    StructField("price",     FloatType(),   True),
    StructField("volume",    FloatType(),   True),
    StructField("high",      FloatType(),   True),
    StructField("low",       FloatType(),   True),
    StructField("timestamp", StringType(),  True),
])

# ── Read from Kafka ────────────────────────────────────────────────────────────
print("📡 Connecting to Kafka...")
raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON messages from Kafka
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("event_time", col("timestamp").cast(TimestampType())) \
 .withColumn("loaded_at", current_timestamp()) \
 .withWatermark("event_time", "30 seconds")

print("✅ Kafka stream connected and parsed")

# ── Write to Snowflake Silver ──────────────────────────────────────────────────
def write_silver(batch_df, batch_id):
    """Write each micro-batch to Snowflake SILVER layer"""
    rows = batch_df.collect()
    if not rows:
        return

    conn = get_snowflake_conn()
    cursor = conn.cursor()

    # Build bulk INSERT
    insert_rows = []
    for row in rows:
        insert_rows.append(f"""
            '{row['symbol']}',
            {row['price']},
            {row['volume']},
            {row['high']},
            {row['low']},
            '{row['event_time']}',
            CURRENT_TIMESTAMP()
        """)

    union_sql = "\nUNION ALL\nSELECT ".join(insert_rows)
    sql = f"""
    INSERT INTO SOCIAL_ANALYTICS.SILVER.CRYPTO_PRICES_CLEAN
    (symbol, price, volume, high, low, event_time, loaded_at)
    SELECT {union_sql}
    """
    cursor.execute(sql)
    conn.commit()
    conn.close()

    print(f"✅ Silver: Loaded {len(rows)} records (batch {batch_id})")

# ── 1-Minute OHLC Aggregation for Gold ────────────────────────────────────────
# OHLC = Open, High, Low, Close — standard financial aggregation
# Groups prices into 1-minute windows per symbol
gold_stream = parsed_stream \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol")
    ) \
    .agg(
        first("price").alias("open_price"),
        max("price").alias("high_price"),
        min("price").alias("low_price"),
        last("price").alias("close_price"),
        spark_round(avg("price"), 2).alias("avg_price"),
        count("price").alias("trade_count")
    )

def write_gold(batch_df, batch_id):
    """Write 1-minute OHLC aggregations to Snowflake GOLD layer"""
    rows = batch_df.collect()
    if not rows:
        return

    conn = get_snowflake_conn()
    cursor = conn.cursor()

    insert_rows = []
    for row in rows:
        minute_bucket = row['window']['start']
        insert_rows.append(f"""
            '{row['symbol']}',
            '{minute_bucket}',
            {row['open_price']},
            {row['high_price']},
            {row['low_price']},
            {row['close_price']},
            {row['avg_price']},
            {row['trade_count']},
            CURRENT_TIMESTAMP()
        """)

    if not insert_rows:
        return

    union_sql = "\nUNION ALL\nSELECT ".join(insert_rows)
    sql = f"""
    INSERT INTO SOCIAL_ANALYTICS.GOLD.CRYPTO_ANALYTICS
    (symbol, minute_bucket, open_price, high_price, low_price,
     close_price, avg_price, trade_count, loaded_at)
    SELECT {union_sql}
    """
    cursor.execute(sql)
    conn.commit()
    conn.close()

    print(f"✅ Gold: Loaded {len(rows)} OHLC records (batch {batch_id})")

# ── Start Streaming Queries ────────────────────────────────────────────────────
print("🚀 Starting Silver stream...")
silver_query = parsed_stream \
    .writeStream \
    .foreachBatch(write_silver) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("🚀 Starting Gold stream...")
gold_query = gold_stream \
    .writeStream \
    .foreachBatch(write_gold) \
    .outputMode("update") \
    .trigger(processingTime="60 seconds") \
    .start()

print("=" * 60)
print("✅ Spark Streaming running!")
print("   Silver: updating every 10 seconds")
print("   Gold:   updating every 60 seconds (1-min OHLC windows)")
print("=" * 60)

# Keep running until Ctrl+C
spark.streams.awaitAnyTermination()