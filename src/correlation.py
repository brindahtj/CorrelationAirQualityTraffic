from typing import List, Optional

from Archive.Api_ingestion.constants import CORRELATION_PRECISION, MIN_CORRELATION_PAIRS


def _filter_valid_pairs(
    x: List[Optional[float]],
    y: List[Optional[float]],
) -> List[tuple]:
    """
    Retourne uniquement les paires où x ET y sont non-nulles.

    Args:
        x: Première série de valeurs
        y: Deuxième série de valeurs

    Returns:
        Liste de tuples (a, b)
    """
    return [(a, b) for a, b in zip(x, y) if a is not None and b is not None]


def _calculate_sums(pairs: List[tuple]) -> tuple:
    """
    Calcule les sommes nécessaires pour Pearson.

    Args:
        pairs: Liste de paires (a, b)

    Returns:
        tuple: (sx, sy, sxy, sx2, sy2)
    """
    sx = sum(a for a, _ in pairs)
    sy = sum(b for _, b in pairs)
    sxy = sum(a * b for a, b in pairs)
    sx2 = sum(a**2 for a, _ in pairs)
    sy2 = sum(b**2 for _, b in pairs)

    return sx, sy, sxy, sx2, sy2


def pearson_correlation(
    x: List[Optional[float]],
    y: List[Optional[float]],
) -> Optional[float]:
    """
    Calcule le coefficient de corrélation de Pearson.

    Args:
        x: Première série (ex: trafic)
        y: Deuxième série (ex: NO2)

    Returns:
        Coefficient de corrélation entre -1 et 1, ou None si insuffisant

    Note:
        Requiert au moins 2 paires valides pour calculer.
    """
    pairs = _filter_valid_pairs(x, y)
    n = len(pairs)

    if n < MIN_CORRELATION_PAIRS:
        return None

    sx, sy, sxy, sx2, sy2 = _calculate_sums(pairs)

    numerator = n * sxy - sx * sy
    denominator = ((n * sx2 - sx**2) * (n * sy2 - sy**2)) ** 0.5

    if denominator == 0:
        return 0.0

    return round(numerator / denominator, CORRELATION_PRECISION)