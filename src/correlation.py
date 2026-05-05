from typing import Optional


# ── helper extrait lors du refactor ──────────────────────────────────────────


def _filter_valid_pairs(
    x: list[Optional[float]],
    y: list[Optional[float]],
) -> list[tuple[float, float]]:
    """Retourne uniquement les paires où x ET y sont non-nulles."""
    return [(a, b) for a, b in zip(x, y) if a is not None and b is not None]


# ── fonction principale ───────────────────────────────────────────────────────


def pearson_correlation(
    trafic: list[Optional[float]],
    no2: list[Optional[float]],
) -> Optional[float]:
    pairs = _filter_valid_pairs(trafic, no2)
    n = len(pairs)

    if n < 2:
        return None

    sx = sum(a for a, _ in pairs)
    sy = sum(b for _, b in pairs)
    sxy = sum(a * b for a, b in pairs)
    sx2 = sum(a**2 for a, _ in pairs)
    sy2 = sum(b**2 for _, b in pairs)

    numerator = n * sxy - sx * sy
    denominator = ((n * sx2 - sx**2) * (n * sy2 - sy**2)) ** 0.5

    if denominator == 0:
        return 0.0

    return round(numerator / denominator, 2)
