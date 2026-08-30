"""Unit tests for src/processing/silver_to_gold.py

All tests run in PySpark local mode — no GCS connection required.
Pure transformation logic is exercised by importing the helper functions
directly, so no patching of I/O is needed.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_local():
    # Pure local session — no Delta extensions needed for transformation unit tests
    # (extensions are only required when reading/writing Delta files, which these
    #  tests don't do; they exercise in-memory DataFrame transformations only)
    return (SparkSession.builder
            .master("local[2]")
            .appName("UnitTest-SilverToGold")
            .getOrCreate())


# ---------------------------------------------------------------------------
# Test 1: dim_tempo derivations
# ---------------------------------------------------------------------------

def test_dim_tempo_derivations(spark_local):
    """Given anos 2024 and 2028, verify decada and periodo_meta derivations."""
    from src.processing.silver_to_gold import _build_dim_tempo

    df = spark_local.createDataFrame([(2024,), (2028,)], ["ano"])
    df_tempo = _build_dim_tempo(df)
    result = {r["ano"]: r for r in df_tempo.collect()}

    # 2024 → decada "2020", belongs to period 2023-2026
    assert result[2024]["decada"] == "2020", (
        f"Expected '2020' but got {result[2024]['decada']!r}"
    )
    assert result[2024]["periodo_meta"] == "2023-2026", (
        f"Expected '2023-2026' but got {result[2024]['periodo_meta']!r}"
    )

    # 2028 → decada "2020", belongs to period 2027-2030
    assert result[2028]["decada"] == "2020", (
        f"Expected '2020' but got {result[2028]['decada']!r}"
    )
    assert result[2028]["periodo_meta"] == "2027-2030", (
        f"Expected '2027-2030' but got {result[2028]['periodo_meta']!r}"
    )


# ---------------------------------------------------------------------------
# Test 2: fato_indicador_alfabetizacao SAEB cutoff flag
# ---------------------------------------------------------------------------

def test_fato_saeb_cutoff_flag(spark_local):
    """media_proficiencia_saeb = 800.0 → atingiu_corte = True, gap = -57.0."""
    from src.processing.silver_to_gold import _build_fato_indicador_alfabetizacao

    sample = [("3550308", "SP", 2024, 800.0)]
    df = spark_local.createDataFrame(
        sample, ["id_municipio", "sigla_uf", "ano", "media_proficiencia_saeb"]
    )
    df_fato = _build_fato_indicador_alfabetizacao(df)
    row = df_fato.collect()[0]

    assert row["atingiu_corte_alfabetizado"] is True, (
        f"Expected True but got {row['atingiu_corte_alfabetizado']!r}"
    )
    # gap = 743.0 - 800.0 = -57.0
    assert row["gap_proficiencia"] == -57.0, (
        f"Expected -57.0 but got {row['gap_proficiencia']!r}"
    )
    # score above cutoff → ADEQUADO
    assert row["status_intervencao"] == "ADEQUADO", (
        f"Expected 'ADEQUADO' but got {row['status_intervencao']!r}"
    )


# ---------------------------------------------------------------------------
# Test 3: fato_metas_vs_resultado status_cumprimento
# ---------------------------------------------------------------------------

def test_fato_metas_status_cumprimento(spark_local):
    """Three rows covering all three status_cumprimento outcomes."""
    from src.processing.silver_to_gold import _build_fato_metas_vs_resultado

    # meta_oficial=100 in all cases
    sample = [
        ("3550308", "SP", 2024, 100.0, 105.0),  # indicador > meta → ATINGIDA
        ("2304400", "CE", 2024, 100.0,  95.0),  # gap = -5 %  → PARCIAL
        ("1302603", "AM", 2024, 100.0,  85.0),  # gap = -15 % → NAO_ATINGIDA
    ]
    df = spark_local.createDataFrame(
        sample,
        ["id_municipio", "sigla_uf", "ano", "meta_alfabetizacao", "indicador_alfabetizacao"]
    )
    df_fato = _build_fato_metas_vs_resultado(df)
    assert df_fato is not None, "Expected a DataFrame, got None (meta_alfabetizacao column missing?)"

    result = {r["id_municipio"]: r["status_cumprimento"] for r in df_fato.collect()}

    assert result["3550308"] == "ATINGIDA", (
        f"Expected 'ATINGIDA' but got {result['3550308']!r}"
    )
    assert result["2304400"] == "PARCIAL", (
        f"Expected 'PARCIAL' but got {result['2304400']!r}"
    )
    assert result["1302603"] == "NAO_ATINGIDA", (
        f"Expected 'NAO_ATINGIDA' but got {result['1302603']!r}"
    )
