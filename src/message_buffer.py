"""
Buffer de messages pour accumuler les données avant traitement.

Respecte SRP : responsabilité unique = gérer un buffer.
"""

from typing import List, Dict, Any

from Archive.Api_ingestion.exceptions import BufferError


class MessageBuffer:
    """Buffer thread-safe pour les messages."""

    def __init__(self, min_size: int = 10):
        """
        Args:
            min_size: Taille minimale avant de déclencher un traitement
        """
        self.min_size = min_size
        self._buffer: List[Dict[str, Any]] = []

    def add(self, message: Dict[str, Any]) -> None:
        """Ajoute un message au buffer."""
        if not isinstance(message, dict):
            raise BufferError(f"Message invalide : {type(message)}")
        self._buffer.append(message)

    def extend(self, messages: List[Dict[str, Any]]) -> None:
        """Ajoute plusieurs messages au buffer."""
        for msg in messages:
            self.add(msg)

    def get_all(self) -> List[Dict[str, Any]]:
        """Récupère tous les messages et vide le buffer."""
        result = self._buffer.copy()
        self._buffer.clear()
        return result

    def size(self) -> int:
        """Taille actuelle du buffer."""
        return len(self._buffer)

    def is_ready(self) -> bool:
        """Vérifie si le buffer a assez de messages."""
        return self.size() >= self.min_size

    def clear(self) -> None:
        """Vide le buffer."""
        self._buffer.clear()