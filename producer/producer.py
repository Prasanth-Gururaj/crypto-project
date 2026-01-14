import json
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer
import websocket

KAFKA_BOOTSTRAP = ['3.235.139.215']  # works from host to Docker Kafka with port 9092 mapped
TOPIC_TICKER = "crypto.ticker.raw"
TOPIC_TRADES = "crypto.trades.raw"

PRODUCT_IDS = ["BTC-USD", "ETH-USD"]
CHANNELS = ["ticker", "market_trades"]

# -------- LOGGING --------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("Producer")

producer = KafkaProducer(bootstrap_servers = KAFKA_BOOTSTRAP,
                         key_serializer = lambda k: k.encode("utf-8"),
                         value_serializer = lambda v: json.dumps(v).encode("utf-8"),
)

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def extract_product_id(msg:dict) -> str | None:
    """
    For Advanced Trade WS:
      - ticker:  msg["events"][0]["tickers"][0]["product_id"]
      - trades:  msg["events"][0]["trades"][0]["product_id"]
    """

    try:
        events = msg.get("events", [])
        if not events:
            return None
        ev0 = events[0]
        if "tickers" in ev0 and ev0["tickers"]:
            return ev0["tickers"][0]["product_id"]
        if "trades" in ev0 and ev0["trades"]:
            return ev0["trades"][0]["product_id"]
    except Exception as e:
        logger.warning("Failed to extract product_id: %s", e)
    return None

def handle_message_dict(data : dict):
    """
    Common handler for parsed JSON dicts from WS.
    Enrich with ingest_ts + source and send to the right Kafka topic.
    """
    channel = data.get("channel")
    if channel not in CHANNELS:
        return  # ignore other channels

    product_id = extract_product_id(data)
    if not product_id:
        logger.warning("No product_id in message: %s", data)
        return
    
    # Add metadata
    data["ingest_ts"] = now_utc_iso()
    data["source"] = "ws_public"

    if channel == "ticker":
        topic = TOPIC_TICKER
    elif channel == "market_trades":
        topic = TOPIC_TRADES
    else:
        return

    try:
        producer.send(topic, key=product_id, value=data)
        # batching/flush handled by producer internally; you can call flush() periodically if you want
        logger.info("Sent %s for %s to topic %s", channel, product_id, topic)
    except Exception as e:
        logger.error("Failed to send to Kafka: %s", e)


# -------- WEBSOCKET CALLBACKS --------
def on_open(ws):
    logger.info("WebSocket opened, subscribing...")

    # Subscribe to ticker
    sub_ticker = {
        "type": "subscribe",
        "product_ids": PRODUCT_IDS,
        "channel": "ticker",
    }
    ws.send(json.dumps(sub_ticker))

    # Subscribe to market_trades
    sub_trades = {
        "type": "subscribe",
        "product_ids": PRODUCT_IDS,
        "channel": "market_trades",
    }
    ws.send(json.dumps(sub_trades))

    logger.info("Subscribed to channels %s for %s", CHANNELS, PRODUCT_IDS)

def on_message(ws, message: str):
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        logger.warning("Non-JSON message: %s", message)
        return

    try:
        handle_message_dict(data)
    except Exception as e:
            logger.exception("Error in handle_message_dict: %s", e)


def on_error(ws, error):
    logger.error("WebSocket error: %s", error)

def on_close(ws, close_status_code, close_msg):
    logger.warning("WebSocket closed: %s %s", close_status_code, close_msg)

def main():
    ws = websocket.WebSocketApp(
        "wss://advanced-trade-ws.coinbase.com",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # run_forever will auto-reconnect by default on many errors;
    # you can add ping_interval, ping_timeout if needed.
    logger.info("Starting WebSocket loop...")
    ws.run_forever()


if __name__ == "__main__":
    try:
        main()
    finally:
        producer.flush()
        producer.close()