"""
Crypto Price Consumer
----------------------
- Reads crypto prices from Kafka topic 'crypto_prices'
- Loads them into Snowflake BRONZE.RAW_CRYPTO_STREAM
- Runs continuously — processes every message as it arrives

Run this script directly:
    python dags/crypto_consumer.py
"""

import json
from kafka import KafkaConsumer
from snowflake.connector import connect
from datetime import datetime

# ── Snowflake Connection ───────────────────────────────────────────────────────
print("✅ Connecting to Snowflake...")
conn = connect(
    account='pm48117.us-east-2.aws',
    user='HIMABINDUT9715',
    password='Pradhyunsai9715!',
    warehouse='AIRFLOW_WH',
    database='SOCIAL_ANALYTICS',
    schema='BRONZE'
)
cursor = conn.cursor()
print("✅ Connected to Snowflake")

# ── Kafka Consumer Setup ───────────────────────────────────────────────────────
consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',     # only read new messages
    group_id='crypto_consumer_group'
)
print("✅ Connected to Kafka — listening for prices...")
print("=" * 60)

# ── Batch settings ─────────────────────────────────────────────────────────────
# Instead of inserting one record at a time,
# we collect 10 messages then insert them all at once (mini-batch)
BATCH_SIZE = 10
batch = []
total_loaded = 0

# ── Main Consumer Loop ─────────────────────────────────────────────────────────
for message in consumer:
    data = message.value

    # Add to batch
    batch.append(data)

    print(f"📩 {data['symbol']} = ${data['price']:,.2f} (batch: {len(batch)}/{BATCH_SIZE})")

    # When batch is full, load to Snowflake
    if len(batch) >= BATCH_SIZE:

        # Build bulk INSERT with all batch records
        rows = []
        for record in batch:
            rows.append(f"""
                '{record['symbol']}',
                {record['price']},
                {record['volume']},
                {record['high']},
                {record['low']},
                '{record['timestamp']}',
                CURRENT_TIMESTAMP()
            """)

        union_sql = "\nUNION ALL\nSELECT ".join(rows)

        sql = f"""
        INSERT INTO SOCIAL_ANALYTICS.BRONZE.RAW_CRYPTO_STREAM
        (symbol, price, volume, high, low, event_time, loaded_at)
        SELECT {union_sql}
        """

        cursor.execute(sql)
        conn.commit()

        total_loaded += len(batch)
        print(f"✅ Loaded batch of {len(batch)} records to Snowflake | Total: {total_loaded}")
        print("-" * 60)

        # Clear batch
        batch = []