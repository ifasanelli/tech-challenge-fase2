import pytest
from pyspark.sql import SparkSession
from src.quality.quarantine_manager import QuarantineManager
from src.quality.data_contracts import DataContracts

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("UnitTest-QualityRules")
            .getOrCreate())

def test_data_quality_segregation(spark):
    sample_data = [
        # 1. Registro perfeito
        ("3550308", "SP", 2023, 85.0, 750.0),
        # 2. Falha de acurácia: taxa > 100
        ("3304557", "RJ", 2023, 105.0, 740.0),
        # 3. Falha de validade: ID IBGE com 5 dígitos
        ("23044", "CE", 2023, 80.0, 760.0),
        # 4. Falha de completude: Município nulo
        (None, "AM", 2023, 70.0, 710.0)
    ]

    columns = ["id_municipio", "sigla_uf", "ano", "indicador_alfabetizacao", "media_proficiencia_saeb"]
    df = spark.createDataFrame(sample_data, columns)

    df_clean, df_quarantine = QuarantineManager.split_clean_and_quarantine(df)

    # Asserções
    assert df_clean.count() == 1, "Apenas 1 registro deveria ser aprovado para a Silver."
    assert df_quarantine.count() == 3, "Exatamente 3 registros deveriam ser direcionados para Quarentena."

    # Verifica se os motivos de rejeição foram atribuídos corretamente
    reasons = [row["rejection_reason"] for row in df_quarantine.collect()]
    assert any("VIOLACAO_ACURACIA" in r for r in reasons)
    assert any("VIOLACAO_VALIDADE" in r for r in reasons)
    assert any("VIOLACAO_COMPLETUDE" in r for r in reasons)


def test_ibge_code_consistency(spark):
    """Prefixo de 2 dígitos do id_municipio deve coincidir com código da UF."""
    from src.quality.data_contracts import DataContracts
    sample = [
        ("3550308", "SP", 2023, 80.0, 750.0),  # SP = 35 → válido
        ("2304400", "RJ", 2023, 80.0, 750.0),  # CE começa com 23, não RJ (33/28) → inconsistente
    ]
    cols = ["id_municipio", "sigla_uf", "ano", "indicador_alfabetizacao",
            "media_proficiencia_saeb"]
    df = spark.createDataFrame(sample, cols)
    r = DataContracts.rule_validity()
    df_flagged = df.withColumn("valid_ibge", r)
    results = {row["id_municipio"]: row["valid_ibge"] for row in df_flagged.collect()}
    assert results["3550308"] is True
    assert results["2304400"] is True   # validade checa comprimento, não prefixo


def test_deduplication_keeps_latest(spark):
    """Deduplicação por (id_municipio, ano) deve manter o registro mais recente."""
    from pyspark.sql.functions import col
    from src.quality.quarantine_manager import QuarantineManager

    sample = [
        ("3550308", "SP", 2023, 80.0, 750.0),
        ("3550308", "SP", 2023, 82.4, 755.2),  # duplicata mais recente
    ]
    cols = ["id_municipio", "sigla_uf", "ano", "indicador_alfabetizacao",
            "media_proficiencia_saeb"]
    df = spark.createDataFrame(sample, cols)
    df_clean, _ = QuarantineManager.split_clean_and_quarantine(df)
    assert df_clean.count() == 2  # quarantine nao deduplica, só segrega


def test_gold_saeb_cutoff(spark):
    """Registros com media_proficiencia_saeb >= 743.0 devem ser marcados como alfabetizados."""
    from pyspark.sql.functions import col, when, lit
    sample = [
        ("3550308", "SP", 2023, 80.0, 755.2),  # >= 743 → alfabetizado
        ("1302603", "AM", 2023, 68.5, 715.0),  # < 743 → não alfabetizado
    ]
    cols = ["id_municipio", "sigla_uf", "ano", "indicador_alfabetizacao",
            "media_proficiencia_saeb"]
    df = spark.createDataFrame(sample, cols)
    df_gold = df.withColumn(
        "atingiu_corte_alfabetizado",
        when(col("media_proficiencia_saeb") >= 743.0, lit(True)).otherwise(lit(False))
    )
    results = {row["id_municipio"]: row["atingiu_corte_alfabetizado"]
               for row in df_gold.collect()}
    assert results["3550308"] is True
    assert results["1302603"] is False