import os
import time
import signal
import json
import pika
import logging
from src.correlation import pearson_correlation

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")
EXCHANGE = os.getenv("RABBIT_EXCHANGE", "logs")

stop_consuming = False
pollution_buffer = []
traffic_buffer = []

def on_message(ch, method, properties, body):
    """Traite les messages de pollution ou trafic."""
    global pollution_buffer, traffic_buffer

    try:
        message = json.loads(body.decode("utf-8"))
        routing_key = method.routing_key

        if routing_key == "pollution":
            pollution_buffer.extend(message if isinstance(message, list) else [message])
            log.info(f"📊 Reçu {len(message) if isinstance(message, list) else 1} mesures pollution")

        elif routing_key == "traffic":
            traffic_buffer.extend(message if isinstance(message, list) else [message])
            log.info(f"🚗 Reçu {len(message) if isinstance(message, list) else 1} mesures trafic")

        if len(pollution_buffer) >= 10 and len(traffic_buffer) >= 10:
            compute_and_publish_correlation()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        log.error(f"Erreur traitement : {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def compute_and_publish_correlation():
    """Calcule la corrélation par ville."""
    global pollution_buffer, traffic_buffer

    no2_by_city = {}
    for reading in pollution_buffer:
        if isinstance(reading, dict) and reading.get("pollutant") == "no2":
            city = reading.get("city")
            no2_by_city.setdefault(city, []).append(reading.get("value"))

    traffic_by_city = {}
    for reading in traffic_buffer:
        if isinstance(reading, dict):
            city = reading.get("city")
            traffic_by_city.setdefault(city, []).append(reading.get("jam_factor"))

    for city in no2_by_city:
        if city in traffic_by_city:
            correlation = pearson_correlation(traffic_by_city[city], no2_by_city[city])
            log.info(f"📈 {city} : Corrélation = {correlation}")

    pollution_buffer.clear()
    traffic_buffer.clear()

def connect_and_consume():
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, virtual_host=RABBIT_VHOST,
        credentials=creds, heartbeat=600, blocked_connection_timeout=300
    )

    for attempt in range(1, 6):
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

            q_pollution = ch.queue_declare(queue="pollution_queue", durable=True).method.queue
            q_traffic = ch.queue_declare(queue="traffic_queue", durable=True).method.queue

            ch.queue_bind(exchange=EXCHANGE, queue=q_pollution, routing_key="pollution")
            ch.queue_bind(exchange=EXCHANGE, queue=q_traffic, routing_key="traffic")

            ch.basic_qos(prefetch_count=10)
            ch.basic_consume(queue=q_pollution, on_message_callback=on_message)
            ch.basic_consume(queue=q_traffic, on_message_callback=on_message)

            log.info("✅ Consommateur démarré")
            while not stop_consuming:
                conn.process_data_events(time_limit=1)
            return

        except Exception as e:
            log.error(f"Tentative {attempt}/5 échouée : {e}")
            time.sleep(2)

def handle_signal(signum, frame):
    global stop_consuming
    stop_consuming = True
    log.info("Arrêt...")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    connect_and_consume()