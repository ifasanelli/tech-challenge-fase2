import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.monitoring.pipeline_monitor import monitor_stage

load_dotenv()

_GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "tech-challenge-fase2-507116")
_GCS_BUCKET = os.environ.get("GCS_BUCKET", "tech-challenge-fase2-507116-datalake")
_MOCK_DIR = Path(__file__).parent.parent.parent / "data" / "mock"

SOURCES = [
    {
        "name": "br_inep_alfabetizacao_brasil",
        "query": "SELECT * FROM `basedosdados.br_inep_alfabetizacao.brasil`",
        "bronze_prefix": "bronze/br_inep_alfabetizacao_brasil",
    },
    {
        "name": "br_inep_alfabetizacao_uf",
        "query": "SELECT * FROM `basedosdados.br_inep_alfabetizacao.uf`",
        "bronze_prefix": "bronze/br_inep_alfabetizacao_uf",
    },
    {
        "name": "br_inep_alfabetizacao_municipio",
        "query": "SELECT * FROM `basedosdados.br_inep_alfabetizacao.municipio`",
        "bronze_prefix": "bronze/br_inep_alfabetizacao_municipio",
    },
    {
        "name": "br_inep_censo_escolar_aluno",
        "query": """
            SELECT id_municipio, sigla_uf, ano, id_escola,
                   localizacao, dependencia_administrativa
            FROM `basedosdados.br_inep_censo_escolar.aluno`
            WHERE ano = 2023
            LIMIT 100000
        """,
        "bronze_prefix": "bronze/br_inep_censo_escolar_aluno",
    },
    {
        "name": "ibge_municipios",
        "query": """
            SELECT id_municipio, nome AS nome_municipio, sigla_uf
            FROM `basedosdados.br_bd_diretorios_brasil.municipio`
        """,
        "bronze_prefix": "bronze/ibge_municipios",
    },
    {
        "name": "ibge_ufs",
        "query": """
            SELECT sigla AS sigla_uf, nome AS nome_uf, nome_regiao AS regiao
            FROM `basedosdados.br_bd_diretorios_brasil.uf`
        """,
        "bronze_prefix": "bronze/ibge_ufs",
    },
]


def _load_mock_fallback(source_name: str) -> pd.DataFrame:
    mock_file = _MOCK_DIR / f"{source_name}.csv"
    if mock_file.exists():
        return pd.read_csv(mock_file, dtype=str)
    return pd.DataFrame()


def _add_lineage(df: pd.DataFrame, source_name: str, batch_id: str) -> pd.DataFrame:
    df = df.copy()
    df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    df["_source_table"] = source_name
    df["_batch_id"] = batch_id
    return df


def _write_to_gcs(df: pd.DataFrame, gcs_path: str) -> None:
    try:
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        with fs.open(gcs_path, "wb") as fh:
            df.to_parquet(fh, index=False, compression="snappy")
    except Exception as exc:
        print(f"[WARN] Falha ao escrever em GCS ({gcs_path}): {exc}")


@monitor_stage("batch_ingestion")
def ingest_batch_to_bronze() -> dict:
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    total_rows = 0

    for source in SOURCES:
        try:
            import basedosdados as bd
            df = bd.read_sql(source["query"], billing_project_id=_GCP_PROJECT)
            print(f"[BD] {source['name']}: {len(df)} registros obtidos via Base dos Dados")
        except Exception as exc:
            print(f"[WARN] Falha ao buscar {source['name']} do Base dos Dados: {exc}")
            print(f"[INFO] Usando fallback mock para {source['name']}")
            df = _load_mock_fallback(source["name"])

        if df.empty:
            print(f"[SKIP] {source['name']}: sem dados, pulando.")
            continue

        df = _add_lineage(df, source["name"], batch_id)
        gcs_path = f"gs://{_GCS_BUCKET}/{source['bronze_prefix']}/data_{batch_id}.parquet"
        _write_to_gcs(df, gcs_path)
        print(f"[✔] {source['name']}: {len(df)} registros → {gcs_path}")
        total_rows += len(df)

    rows_q = 0
    rate = 0.0
    return {
        "rows_input": total_rows,
        "rows_clean": total_rows,
        "rows_quarantined": rows_q,
        "quarantine_rate_pct": rate,
    }


if __name__ == "__main__":
    ingest_batch_to_bronze()