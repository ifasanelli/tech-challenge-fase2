import os
from dotenv import load_dotenv
from pyspark.sql.functions import col, length, lpad, upper, row_number, current_timestamp
from pyspark.sql.window import Window

from src.monitoring.pipeline_monitor import monitor_stage
from src.quality.quarantine_manager import QuarantineManager
from src.utils.spark_utils import get_spark_gcs

load_dotenv()

_BUCKET = os.environ.get("GCS_BUCKET", "tech-challenge-fase2-507116-datalake")
_BASE = f"gs://{_BUCKET}"


def _read_bronze(spark, prefix: str):
    path = f"{_BASE}/{prefix}/"
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        print(f"[WARN] Não foi possível ler {path}: {exc}")
        return None


def _normalize(df):
    df = df.withColumn("id_municipio", lpad(col("id_municipio"), 7, "0"))
    df = df.withColumn("sigla_uf", upper(col("sigla_uf")))
    return df


def _deduplicate(df, partition_cols: list, order_col: str):
    w = Window.partitionBy(*partition_cols).orderBy(col(order_col).desc())
    return (df.withColumn("_rn", row_number().over(w))
              .filter(col("_rn") == 1)
              .drop("_rn"))


@monitor_stage("bronze_to_silver")
def process_bronze_to_silver() -> dict:
    spark = get_spark_gcs("Medallion-BronzeToSilver")
    rows_input = 0
    rows_quarantined = 0

    # 1. Ler fontes Bronze
    df_metas = _read_bronze(spark, "bronze/br_inep_alfabetizacao_municipio")
    df_ibge_mun = _read_bronze(spark, "bronze/ibge_municipios")
    df_ibge_uf = _read_bronze(spark, "bronze/ibge_ufs")
    df_stream = _read_bronze(spark, "bronze/medicoes_stream")

    if df_metas is None:
        raise RuntimeError("Bronze br_inep_alfabetizacao_municipio não encontrado — execute batch_ingestion primeiro.")

    rows_input += df_metas.count()

    # 2. Normalizar e aplicar qualidade nas metas
    df_metas = _normalize(df_metas)
    df_clean_metas, df_quarantine = QuarantineManager.split_clean_and_quarantine(df_metas)
    rows_quarantined += df_quarantine.count()

    # 3. Enriquecimento com dados territoriais IBGE
    if df_ibge_mun is not None:
        mun_cols = ["id_municipio"]
        if "nome_municipio" in df_ibge_mun.columns:
            mun_cols.append("nome_municipio")
        df_clean_metas = df_clean_metas.join(
            df_ibge_mun.select(mun_cols), "id_municipio", "left"
        )

    if df_ibge_uf is not None:
        uf_cols = ["sigla_uf"]
        for c in ["nome_uf", "regiao"]:
            if c in df_ibge_uf.columns:
                uf_cols.append(c)
        df_clean_metas = df_clean_metas.join(
            df_ibge_uf.select(uf_cols), "sigla_uf", "left"
        )

    # 4. Deduplicação
    dedup_col = "_ingestion_timestamp" if "_ingestion_timestamp" in df_clean_metas.columns else "ano"
    df_silver_metas = _deduplicate(df_clean_metas, ["id_municipio", "ano"], dedup_col)

    # 5. Processar stream (se disponível)
    if df_stream is not None:
        rows_input += df_stream.count()
        df_stream = _normalize(df_stream)
        df_clean_stream, df_quarantine_stream = QuarantineManager.split_clean_and_quarantine(df_stream)
        rows_quarantined += df_quarantine_stream.count()
        df_silver_stream = _deduplicate(df_clean_stream, ["id_municipio", "ano"],
                                        "_ingestion_timestamp" if "_ingestion_timestamp" in df_clean_stream.columns else "ano")

        # Quarentena do stream
        (df_quarantine_stream.withColumn("_quarantine_timestamp", current_timestamp())
         .write.format("delta").mode("append")
         .save(f"{_BASE}/silver/quarantine/"))

        # Silver stream
        (df_silver_stream.write.format("delta").mode("overwrite")
         .partitionBy("sigla_uf")
         .save(f"{_BASE}/silver/fato_medicoes_stream/"))

    # 6. Gravar quarentena das metas
    (df_quarantine.withColumn("_quarantine_timestamp", current_timestamp())
     .write.format("delta").mode("append")
     .save(f"{_BASE}/silver/quarantine/"))

    # 7. Gravar Silver metas
    rows_clean = df_silver_metas.count()
    (df_silver_metas.write.format("delta").mode("overwrite")
     .partitionBy("sigla_uf")
     .save(f"{_BASE}/silver/fato_metas_municipio/"))

    quarantine_rate = round((rows_quarantined / rows_input * 100) if rows_input > 0 else 0.0, 2)
    print(f"[✔] Silver: {rows_clean} registros válidos | {rows_quarantined} em quarentena ({quarantine_rate}%)")

    return {
        "rows_input": rows_input,
        "rows_clean": rows_clean,
        "rows_quarantined": rows_quarantined,
        "quarantine_rate_pct": quarantine_rate,
    }


if __name__ == "__main__":
    process_bronze_to_silver()
