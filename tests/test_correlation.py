import pytest
from correlation import pearson_correlation


def test_zone_critique_detectee():
    """
    Scenario: zone A — trafic et NO2 montent ensemble toute la matinée.
    Attendu : coefficient >= 0.7 → zone classée 'forte corrélation'.
    """
    trafic = [820, 1540, 2100, 1980, 1300, 1100, 1420, 1600, 1850, 2000]
    no2 = [28, 45, 62, 58, 38, 33, 42, 49, 55, 60]

    r = pearson_correlation(trafic, no2)

    assert r is not None, "Le calcul ne doit pas retourner None sur données complètes"
    assert r >= 0.7, f"Zone critique attendue (r >= 0.7), obtenu r={r}"


def test_correlation_parfaite_positive():
    """r = 1.00 quand x et y sont linéairement liés."""
    assert pearson_correlation([1, 2, 3, 4, 5], [2, 4, 6, 8, 10]) == 1.00


def test_correlation_parfaite_negative():
    """r = -1.00 quand la relation est inverse."""
    assert pearson_correlation([1, 2, 3, 4, 5], [10, 8, 6, 4, 2]) == -1.00


def test_resultat_arrondi_2_decimales():
    """DoD Task Technique : arrondi à 2 décimales."""
    r = pearson_correlation([1, 2, 3, 4, 5], [1, 3, 2, 5, 4])
    assert r is not None
    # vérifie que la chaîne ne dépasse pas 2 décimales
    decimal_part = str(abs(r)).split(".")[-1] if "." in str(r) else ""
    assert len(decimal_part) <= 2, f"Trop de décimales : {r}"


def test_moins_de_2_paires_retourne_none():
    """Sans données suffisantes, le calcul est impossible."""
    assert pearson_correlation([1, None, None], [2, None, None]) is None
