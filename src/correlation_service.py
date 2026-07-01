"""
Service de calcul de corrélation pollution-trafic.

Respecte SRP : responsabilité unique = calculer la corrélation.
"""

import logging
from typing import Dict, List

from Archive.Api_ingestion.exceptions import CorrelationError
from correlation import pearson_correlation

log = logging.getLogger(__name__)


class CorrelationService:
    """Service pour calculer les corrélations entre pollution et trafic."""

    def __init__(self, pollutant: str = "no2"):
        """
        Args:
            pollutant: Polluant à corréler avec le trafic
        """
        self.pollutant = pollutant.lower()

    def compute_by_city(
        self,
        pollution_readings: List[Dict],
        traffic_readings: List[Dict],
    ) -> Dict[str, float]:
        """
        Calcule la corrélation pour chaque ville.

        Args:
            pollution_readings: Liste des mesures pollution
            traffic_readings: Liste des mesures trafic

        Returns:
            Dict : {city: correlation_value}

        Raises:
            CorrelationError: En cas d'erreur de calcul
        """
        try:
            pollution_by_city = self._extract_values_by_city(
                pollution_readings, self.pollutant
            )
            traffic_by_city = self._extract_values_by_city(
                traffic_readings, "jam_factor"
            )

            correlations = {}
            for city in pollution_by_city:
                if city in traffic_by_city:
                    corr = pearson_correlation(
                        traffic_by_city[city],
                        pollution_by_city[city],
                    )
                    if corr is not None:
                        correlations[city] = corr
                        log.info(f"📈 {city} : Corrélation = {corr}")

            return correlations
        except Exception as exc:
            raise CorrelationError(f"Erreur calcul corrélation : {exc}") from exc

    @staticmethod
    def _extract_values_by_city(
        readings: List[Dict],
        field: str,
    ) -> Dict[str, List[float]]:
        """
        Extrait les valeurs d'un champ groupées par ville.

        Args:
            readings: Liste des lectures
            field: Nom du champ à extraire

        Returns:
            Dict : {city: [value1, value2, ...]}
        """
        by_city = {}
        for reading in readings:
            if not isinstance(reading, dict):
                continue

            city = reading.get("city")
            value = reading.get(field)

            if city and value is not None:
                if city not in by_city:
                    by_city[city] = []
                by_city[city].append(float(value))

        return by_city