import os
import time
import signal
import sys
import json
import pika
from MoteurCorrelation.src.correlation import pearson_correlation

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")
QUEUE_NAME = os.getenv("RABBIT_QUEUE", "moteur_correlation")
EXCHANGE = os.getenv("RABBIT_EXCHANGE", "logs")
RETRIES = int(os.getenv("RABBIT_RETRIES", "5"))
RETRY_DELAY = float(os.getenv("RABBIT_RETRY_DELAY", "2"))

stop_consuming = False

# Historique pour le calcul de corrélation
data_history = {
    "trafic": [],
    "no2": []
}

def process_message(body: bytes):
    """Traite le message et calcule la corrélation."""
    try:
        message = json.loads(body.decode("utf-8"))

        # Extraire les données pertinentes
        trafic = message.get("trafic")
        no2 = message.get("no2")

        if trafic is not None and no2 is not None:
            data_history["trafic"].append(trafic)
            data_history["no2"].append(no2)

            # Calculer la corrélation si suffisamment de données
            correlation = pearson_correlation(
                data_history["trafic"],
                data_history["no2"]
            )

            if correlation is not None:
                print(f"Corrélation Trafic/NO2: {correlation}")
            else:
                print("Données insuffisantes pour la corrélation")
        else:
            print("Données manquantes (trafic ou no2)")

    except json.JSONDecodeError as e:
        print(f"Erreur décodage JSON: {e}")

def on_message(ch, method, properties, body):
    try:
        process_message(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Erreur traitement: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def connect_and_consume():
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host=RABBIT_VHOST,
        credentials=creds,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
            ch.queue_declare(queue=QUEUE_NAME, durable=True)
            ch.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
            print("En écoute sur queue:", QUEUE_NAME)
            while not stop_consuming:
                conn.process_data_events(time_limit=1)
            ch.close()
            conn.close()
            return
        except Exception as e:
            last_exc = e
            print(f"Connexion échouée (tentative {attempt}/{RETRIES}): {e}")
            time.sleep(RETRY_DELAY)
    raise RuntimeError("Impossible de se connecter à RabbitMQ") from last_exc

def handle_sigint(signum, frame):
    global stop_consuming
    stop_consuming = True
    print("Arrêt demandé, fermeture...")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)
    connect_and_consume()