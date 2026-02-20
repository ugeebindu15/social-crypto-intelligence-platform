"""
Crypto Price Producer (Coinbase)
---------------------------------
- Connects to Coinbase WebSocket (free, no API key, works in US)
- Receives BTC, ETH, SOL prices in real time
- Publishes each price to Kafka topic 'crypto_prices'

Run this script directly:
    python dags/crypto_producer.py
"""

import json
import websocket
from kafka import KafkaProducer
from datetime import datetime

# ── Kafka Producer Setup ───────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("✅ Connected to Kafka")

message_count = 0

def on_message(ws, message):
    """Called every time Coinbase sends a price update"""
    global message_count

    data = json.loads(message)
    print(f"📩 Raw message type: {data.get('type')} | keys: {list(data.keys())}")

    # Handle different message types
    msg_type = data.get('type')

    # Coinbase Advanced Trade sends 'subscriptions' on connect
    if msg_type == 'subscriptions':
        print("✅ Subscription confirmed!")
        return

    # Price update comes as 'ticker' or inside 'events'
    if msg_type == 'ticker':
        process_ticker(data)
        return

    # New Coinbase format wraps events in an array
    if 'events' in data:
        for event in data.get('events', []):
            for ticker in event.get('tickers', []):
                process_ticker(ticker)

def process_ticker(ticker):
    """Process a single ticker price update"""
    global message_count

    price = ticker.get('price') or ticker.get('last_trade_price')
    symbol = ticker.get('product_id') or ticker.get('symbol')

    if not price or not symbol:
        return

    price_event = {
        'symbol':    symbol,
        'price':     float(price),
        'volume':    float(ticker.get('volume_24_h', 0) or ticker.get('volume_24h', 0) or 0),
        'high':      float(ticker.get('high_24_h', 0) or ticker.get('high_24h', 0) or 0),
        'low':       float(ticker.get('low_24_h', 0) or ticker.get('low_24h', 0) or 0),
        'timestamp': datetime.utcnow().isoformat()
    }

    # Send to Kafka
    producer.send('crypto_prices', value=price_event)
    producer.flush()

    message_count += 1
    print(f"📨 [{message_count}] {price_event['symbol']} = ${float(price_event['price']):,.2f} → Kafka")

def on_error(ws, error):
    print(f"❌ WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔌 WebSocket closed")

def on_open(ws):
    """Subscribe to BTC, ETH, SOL price feeds"""
    print("✅ Connected to Coinbase WebSocket — subscribing to prices...")

    # Try both old and new Coinbase subscription format
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD"],
        "channel": "ticker"
    }
    ws.send(json.dumps(subscribe_msg))
    print("✅ Subscribed to BTC-USD, ETH-USD, SOL-USD")

def on_error(ws, error):
    print(f"❌ Error: {error}")

def on_close(ws, code, msg):
    print(f"🔌 Closed: {code} {msg}")

# ── Start Streaming ────────────────────────────────────────────────────────────
url = "wss://advanced-trade-ws.coinbase.com"

ws = websocket.WebSocketApp(
    url,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open
)

print("Starting crypto price stream...")
ws.run_forever()