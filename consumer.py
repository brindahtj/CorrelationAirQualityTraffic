import pika
import json
import logging
from dataclasses import asdict
from src.correlation import pearson_correlation

log = logging.getLogger(__name__)

RABBIT_HOST = "localhost"
RABBIT_PORT = 5672
RABBIT_USER = "guest"
RABBIT_PASS = "guest"
RABBIT_VHOST = "/"
EXCHANGE = "logs"

pollution_buffer = []
traffic_buffer = []

def callback(ch, method, properties, body):
    """Traite les messages reçus de RabbitMQ."""
    global pollution_buffer, traffic_buffer

    try:
        message = json.loads(body)
        routing_key = method.routing_key

        if routing_key == "pollution":
            pollution_buffer.extend(message)
            log.info(f"📊 Reçu {len(message)} mesures pollution")

        elif routing_key == "traffic":
            traffic_buffer.extend(message)
            log.info(f"🚗 Reçu {len(message)} mesures trafic")

        # Calculer corrélation si on a suffisamment de données
        if len(pollution_buffer) >= 10 and len(traffic_buffer) >= 10:
            compute_and_publish_correlation()

    except Exception as e:
        log.error(f"Erreur traitement message : {e}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def compute_and_publish_correlation():
    """Calcule la corrélation entre pollution et trafic."""
    global pollution_buffer, traffic_buffer

    # Filtrer NO2 par ville
    no2_by_city = {}
    for reading in pollution_buffer:
        if reading["pollutant"] == "no2":
            city = reading["city"]
            no2_by_city.setdefault(city, []).append(reading["value"])

    # Trafic par ville
    traffic_by_city = {}
    for reading in traffic_buffer:
        city = reading["city"]
        traffic_by_city.setdefault(city, []).append(reading["jam_factor"])

    # Calculer corrélation par ville
    for city in no2_by_city:
        if city in traffic_by_city:
            no2_values = no2_by_city[city]
            traffic_values = traffic_by_city[city]

            correlation = pearson_correlation(traffic_values, no2_values)
            log.info(f"📈 {city} : Corrélation NO2/Trafic = {correlation}")

    # Réinitialiser buffers
    pollution_buffer = []
    traffic_buffer = []


def start_consumer():
    """Démarre le consommateur RabbitMQ."""
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host=RABBIT_VHOST,
        credentials=creds,
    )

    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    # Créer exchange et queues
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    q_pollution = ch.queue_declare(queue="pollution_queue", durable=True).method.queue
    q_traffic = ch.queue_declare(queue="traffic_queue", durable=True).method.queue

    ch.queue_bind(exchange=EXCHANGE, queue=q_pollution, routing_key="pollution")
    ch.queue_bind(exchange=EXCHANGE, queue=q_traffic, routing_key="traffic")

    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue=q_pollution, on_message_callback=callback)
    ch.basic_consume(queue=q_traffic, on_message_callback=callback)

    log.info("🔄 Consommateur démarré, en attente de messages...")
    ch.start_consuming()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_consumer()