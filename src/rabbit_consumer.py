"""
Consumer RabbitMQ pour recevoir les messages.

Respecte SRP : responsabilité unique = consommer et router les messages.
"""

import logging
from typing import Callable

import pika

from Api_ingestion.config import (
    EXCHANGE,
    RABBIT_HOST,
    RABBIT_PASS,
    RABBIT_PORT,
    RABBIT_USER,
    RABBIT_VHOST,
)
from Api_ingestion.constants import ROUTING_KEY_POLLUTION, ROUTING_KEY_TRAFFIC
from Api_ingestion.exceptions import ConsumerError

log = logging.getLogger(__name__)


class RabbitConsumer:
    """Consumer RabbitMQ pour pollution et trafic."""

    def __init__(
        self,
        host: str = RABBIT_HOST,
        port: int = RABBIT_PORT,
        user: str = RABBIT_USER,
        password: str = RABBIT_PASS,
        vhost: str = RABBIT_VHOST,
        heartbeat: int = 600,
        blocked_timeout: int = 300,
    ):
        """
        Args:
            host: Host RabbitMQ
            port: Port RabbitMQ
            user: Utilisateur RabbitMQ
            password: Mot de passe RabbitMQ
            vhost: Virtual host RabbitMQ
            heartbeat: Heartbeat en secondes
            blocked_timeout: Timeout de blocage en secondes
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.heartbeat = heartbeat
        self.blocked_timeout = blocked_timeout
        self._connection = None
        self._channel = None

    def connect(self) -> None:
        """Établit la connexion à RabbitMQ."""
        try:
            creds = pika.PlainCredentials(self.user, self.password)
            params = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhost,
                credentials=creds,
                heartbeat=self.heartbeat,
                blocked_connection_timeout=self.blocked_timeout,
            )
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
            log.info("✅ Connecté à RabbitMQ")
        except pika.exceptions.AMQPError as exc:
            raise ConsumerError(f"Erreur connexion RabbitMQ : {exc}") from exc

    def setup_queues(self) -> tuple:
        """
        Configure l'exchange et les queues.

        Returns:
            tuple: (pollution_queue, traffic_queue)
        """
        try:
            self._channel.exchange_declare(
                exchange=EXCHANGE, exchange_type="topic", durable=True
            )

            q_pollution = self._channel.queue_declare(
                queue="pollution_queue", durable=True
            ).method.queue
            q_traffic = self._channel.queue_declare(
                queue="traffic_queue", durable=True
            ).method.queue

            self._channel.queue_bind(
                exchange=EXCHANGE,
                queue=q_pollution,
                routing_key=ROUTING_KEY_POLLUTION,
            )
            self._channel.queue_bind(
                exchange=EXCHANGE,
                queue=q_traffic,
                routing_key=ROUTING_KEY_TRAFFIC,
            )

            log.info(f"✅ Queues configurées : {q_pollution}, {q_traffic}")
            return q_pollution, q_traffic
        except pika.exceptions.AMQPError as exc:
            raise ConsumerError(f"Erreur configuration queues : {exc}") from exc

    def consume(
        self,
        queues: tuple,
        callback: Callable,
        prefetch_count: int = 10,
    ) -> None:
        """
        Lance la consommation des messages.

        Args:
            queues: Tuple (pollution_queue, traffic_queue)
            callback: Fonction appelée pour chaque message
            prefetch_count: Nombre de messages à prefetch
        """
        q_pollution, q_traffic = queues

        try:
            self._channel.basic_qos(prefetch_count=prefetch_count)
            self._channel.basic_consume(queue=q_pollution, on_message_callback=callback)
            self._channel.basic_consume(queue=q_traffic, on_message_callback=callback)

            log.info("🎯 En attente de messages...")
            self._channel.start_consuming()
        except pika.exceptions.AMQPError as exc:
            raise ConsumerError(f"Erreur consommation : {exc}") from exc

    def stop(self) -> None:
        """Arrête la consommation et ferme la connexion."""
        if self._channel and self._channel.is_open:
            self._channel.stop_consuming()
        if self._connection and self._connection.is_open:
            self._connection.close()
        log.info("✅ Consumer fermé")