"""
Service principal pour le subscriber.

Coordonne la consommation RabbitMQ et les calculs de corrélation.
Respecte SRP : orchestre les composants.
"""

import json
import logging
from typing import Optional

from Api_ingestion.exceptions import ConsumerError
from correlation_service import CorrelationService
from message_buffer import MessageBuffer
from rabbit_consumer import RabbitConsumer

log = logging.getLogger(__name__)


class SubscriberService:
    """Orchestrateur principal pour la consommation et corrélation."""

    def __init__(
        self,
        buffer_size: int = 10,
        pollutant: str = "no2",
    ):
        """
        Args:
            buffer_size: Taille min des buffers avant traitement
            pollutant: Polluant à corréler
        """
        self.pollution_buffer = MessageBuffer(min_size=buffer_size)
        self.traffic_buffer = MessageBuffer(min_size=buffer_size)
        self.correlation_service = CorrelationService(pollutant=pollutant)
        self.consumer = RabbitConsumer()

    def on_message(
        self,
        ch,
        method,
        properties,
        body: bytes,
    ) -> None:
        """
        Callback pour les messages RabbitMQ.

        Args:
            ch: Channel RabbitMQ
            method: Propriétés du message
            properties: Propriétés additionnelles
            body: Corps du message
        """
        try:
            message = self._parse_message(body)
            routing_key = method.routing_key

            if routing_key == "pollution":
                self.pollution_buffer.extend(
                    message if isinstance(message, list) else [message]
                )
                log.info(f"📊 {len(message if isinstance(message, list) else [message])} mesures pollution reçues")

            elif routing_key == "traffic":
                self.traffic_buffer.extend(
                    message if isinstance(message, list) else [message]
                )
                log.info(f"🚗 {len(message if isinstance(message, list) else [message])} mesures trafic reçues")

            # Déclenche la corrélation si les buffers sont prêts
            if self.pollution_buffer.is_ready() and self.traffic_buffer.is_ready():
                self.compute_and_publish_correlation()

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except ConsumerError as exc:
            log.error(f"Erreur métier : {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as exc:
            log.exception(f"Erreur inattendue : {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def compute_and_publish_correlation(self) -> None:
        """Calcule et affiche la corrélation."""
        try:
            pollution = self.pollution_buffer.get_all()
            traffic = self.traffic_buffer.get_all()

            if pollution and traffic:
                correlations = self.correlation_service.compute_by_city(
                    pollution, traffic
                )
                log.info(f"Corrélations calculées : {correlations}")
        except Exception as exc:
            log.error(f"Erreur corrélation : {exc}")

    @staticmethod
    def _parse_message(body: bytes) -> any:
        """Parse un message JSON."""
        try:
            return json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ConsumerError(f"Message JSON invalide : {exc}") from exc

    def start(self, max_retries: int = 5) -> None:
        """Démarre le consumer avec gestion des reconnexions."""
        for attempt in range(1, max_retries + 1):
            try:
                self.consumer.connect()
                queues = self.consumer.setup_queues()
                self.consumer.consume(queues, callback=self.on_message)
                return
            except ConsumerError as exc:
                log.error(f"Tentative {attempt}/{max_retries} échouée : {exc}")
                if attempt < max_retries:
                    import time
                    time.sleep(2)
                else:
                    raise

    def stop(self) -> None:
        """Arrête le consumer."""
        self.consumer.stop()