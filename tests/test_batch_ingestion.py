import os
import pytest
import pandas as pd


def test_load_mock_fallback_retorna_dataframe():
    from src.ingestion.batch_ingestion import _load_mock_fallback
    df = _load_mock_fallback("br_inep_alfabetizacao_municipio")
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "id_municipio" in df.columns


def test_add_lineage_adiciona_colunas_obrigatorias():
    from src.ingestion.batch_ingestion import _add_lineage
    df = pd.DataFrame({"id_municipio": ["3550308"], "ano": [2023]})
    result = _add_lineage(df, "test_source", "20260830_120000")
    assert "_ingestion_timestamp" in result.columns
    assert "_source_table" in result.columns
    assert result["_source_table"].iloc[0] == "test_source"
    assert result["_batch_id"].iloc[0] == "20260830_120000"


def test_mock_fallback_all_sources():
    from src.ingestion.batch_ingestion import SOURCES, _load_mock_fallback
    for source in SOURCES:
        df = _load_mock_fallback(source["name"])
        assert isinstance(df, pd.DataFrame), f"Fallback falhou para {source['name']}"
        assert len(df) > 0, f"Fallback vazio para {source['name']}"
