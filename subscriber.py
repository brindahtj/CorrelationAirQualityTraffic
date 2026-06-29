"""
Point d'entrée pour le subscriber RabbitMQ.

Orchestration simple et lisible.
"""

import logging
import signal
import sys

from src.subscriber_service import SubscriberService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class SubscriberApp:
    """Application subscriber."""

    def __init__(self):
        self.service = SubscriberService(buffer_size=10, pollutant="no2")
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Configure les handlers de signaux."""
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame) -> None:
        """Gère les signaux d'arrêt."""
        log.info("🛑 Arrêt du subscriber...")
        self.service.stop()
        sys.exit(0)

    def run(self) -> None:
        """Lance le subscriber."""
        try:
            log.info("🚀 Démarrage du subscriber...")
            self.service.start()
        except KeyboardInterrupt:
            log.info("Arrêt par utilisateur")
        except Exception as exc:
            log.exception(f"Erreur fatale : {exc}")
            sys.exit(1)


if __name__ == "__main__":
    app = SubscriberApp()
    app.run()