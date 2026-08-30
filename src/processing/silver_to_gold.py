import os

from dotenv import load_dotenv
from pyspark.sql.functions import (
    col, when, lit,
    round as spark_round,
    floor as spark_floor,
    avg, countDistinct,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.monitoring.pipeline_monitor import monitor_stage
from src.utils.spark_utils import get_spark_gcs

load_dotenv()

_BUCKET = os.environ.get("GCS_BUCKET", "tech-challenge-fase2-507116-datalake")
_BASE = f"gs://{_BUCKET}"
_SAEB_CUTOFF = 743.0


# ---------------------------------------------------------------------------
# Helpers — each has one responsibility
# ---------------------------------------------------------------------------

def _read_silver(spark, path: str):
    """Read a Delta table, returning None gracefully on failure."""
    try:
        return spark.read.format("delta").load(path)
    except Exception as exc:
        print(f"[WARN] Não foi possível ler {path}: {exc}")
        return None


def _write_delta(df, path: str, partition_by=None):
    """Write a DataFrame as Delta, optionally partitioned."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def _build_dim_tempo(df_silver):
    """Build dim_tempo: ano → decada, periodo_meta."""
    return (df_silver
            .select("ano")
            .dropDuplicates(["ano"])
            .withColumn("decada", (spark_floor(col("ano") / 10) * 10).cast("string"))
            .withColumn(
                "periodo_meta",
                when((col("ano") >= 2023) & (col("ano") <= 2026), lit("2023-2026"))
                .otherwise(lit("2027-2030"))
            ))


def _build_dim_municipio(df_silver):
    """Build dim_municipio from Silver; includes nome_municipio if present."""
    mun_cols = ["id_municipio", "sigla_uf"]
    for c in ["nome_municipio"]:
        if c in df_silver.columns:
            mun_cols.append(c)
    return df_silver.select(mun_cols).dropDuplicates(["id_municipio"])


def _build_dim_uf(df_silver):
    """Build dim_uf from Silver; includes nome_uf and regiao if present."""
    uf_cols = ["sigla_uf"]
    for c in ["nome_uf", "regiao"]:
        if c in df_silver.columns:
            uf_cols.append(c)
    return df_silver.select(uf_cols).dropDuplicates(["sigla_uf"])


def _build_dim_contexto_escolar(spark, df_censo):
    """Build dim_contexto_escolar from Censo Escolar Bronze.

    Falls back to an empty DataFrame with the canonical schema when the
    Bronze path is unavailable.
    """
    empty_schema = StructType([
        StructField("id_municipio", StringType(), True),
        StructField("taxa_infraestrutura_basica", DoubleType(), True),
        StructField("localizacao_predominante", StringType(), True),
    ])

    if df_censo is None:
        return spark.createDataFrame([], empty_schema)

    try:
        # tp_localizacao: 1 = Urbana, 2 = Rural (INEP convention)
        infra_col = (
            avg(when(col("in_agua_potavel") == 1, lit(1.0)).otherwise(lit(0.0))) * 100
            if "in_agua_potavel" in df_censo.columns
            else lit(None).cast(DoubleType())
        )
        return (df_censo
                .groupBy("id_municipio")
                .agg(
                    spark_round(infra_col, 2).alias("taxa_infraestrutura_basica"),
                    (avg(when(col("tp_localizacao") == 1, lit(1.0)).otherwise(lit(0.0))) * 100
                     ).alias("_perc_urbano"),
                )
                .withColumn(
                    "localizacao_predominante",
                    when(col("_perc_urbano") >= 70, lit("Urbana"))
                    .when(col("_perc_urbano") <= 30, lit("Rural"))
                    .otherwise(lit("Mista"))
                )
                .select("id_municipio", "taxa_infraestrutura_basica", "localizacao_predominante")
                .dropDuplicates(["id_municipio"]))
    except Exception as exc:
        print(f"[WARN] Falha ao derivar dim_contexto_escolar: {exc}")
        return spark.createDataFrame([], empty_schema)


# ---------------------------------------------------------------------------
# Fact table builders
# ---------------------------------------------------------------------------

def _build_fato_indicador_alfabetizacao(df_silver):
    """Build fato_indicador_alfabetizacao with SAEB cutoff rules.

    gap_proficiencia = nota_corte_saeb - media_proficiencia_saeb
    status_intervencao: CRITICO if gap > 50, ATENCAO if gap > 0, ADEQUADO otherwise
    """
    df = (df_silver
          .withColumn("nota_corte_saeb", lit(_SAEB_CUTOFF))
          .withColumn(
              "gap_proficiencia",
              spark_round(lit(_SAEB_CUTOFF) - col("media_proficiencia_saeb"), 2)
          )
          .withColumn(
              "atingiu_corte_alfabetizado",
              col("media_proficiencia_saeb") >= _SAEB_CUTOFF
          )
          .withColumn(
              "status_intervencao",
              when(col("gap_proficiencia") > 50.0, lit("CRITICO"))
              .when(col("gap_proficiencia") > 0.0, lit("ATENCAO"))
              .otherwise(lit("ADEQUADO"))
          ))

    base_cols = [
        "id_municipio", "sigla_uf", "ano",
        "media_proficiencia_saeb", "nota_corte_saeb",
        "gap_proficiencia", "atingiu_corte_alfabetizado", "status_intervencao",
    ]
    # Include optional enrichment columns when present in Silver
    for c in ["indicador_alfabetizacao", "total_alunos_avaliados"]:
        if c in df.columns:
            base_cols.append(c)

    return df.select(base_cols)


def _build_fato_metas_vs_resultado(df_silver):
    """Build fato_metas_vs_resultado comparing indicador_real vs meta_oficial.

    Returns None if meta_alfabetizacao column is absent from Silver.
    gap_percentual = ((indicador_real - meta_oficial) / meta_oficial) * 100
    status_cumprimento: ATINGIDA / PARCIAL (gap >= -10%) / NAO_ATINGIDA
    """
    if "meta_alfabetizacao" not in df_silver.columns:
        return None

    return (df_silver
            .select(
                "id_municipio", "sigla_uf", "ano",
                col("meta_alfabetizacao").alias("meta_oficial"),
                col("indicador_alfabetizacao").alias("indicador_real"),
            )
            .withColumn(
                "gap_percentual",
                spark_round(
                    (col("indicador_real") - col("meta_oficial")) / col("meta_oficial") * 100.0,
                    2
                )
            )
            .withColumn(
                "status_cumprimento",
                when(col("indicador_real") >= col("meta_oficial"), lit("ATINGIDA"))
                .when(col("gap_percentual") >= -10.0, lit("PARCIAL"))
                .otherwise(lit("NAO_ATINGIDA"))
            ))


# ---------------------------------------------------------------------------
# Main pipeline function
# ---------------------------------------------------------------------------

@monitor_stage("silver_to_gold")
def process_silver_to_gold() -> dict:
    """Read Silver Delta and produce 6 Gold Delta tables (Star Schema).

    Returns the monitoring dict required by @monitor_stage.
    """
    spark = get_spark_gcs("Medallion-SilverToGold")

    silver_path = f"{_BASE}/silver/fato_metas_municipio/"
    df_silver = _read_silver(spark, silver_path)
    if df_silver is None:
        raise RuntimeError(
            f"Silver não encontrada em {silver_path} — execute bronze_to_silver primeiro."
        )

    rows_input = df_silver.count()
    rows_clean = 0

    # Opcional: Bronze Censo Escolar
    try:
        df_censo = spark.read.parquet(f"{_BASE}/bronze/br_inep_censo_escolar_aluno/")
    except Exception:
        df_censo = None

    # ---- Dimensions --------------------------------------------------------
    df_dim_tempo = _build_dim_tempo(df_silver)
    _write_delta(df_dim_tempo, f"{_BASE}/gold/dim_tempo/", partition_by=["ano"])

    df_dim_municipio = _build_dim_municipio(df_silver)
    _write_delta(df_dim_municipio, f"{_BASE}/gold/dim_municipio/")

    df_dim_uf = _build_dim_uf(df_silver)
    _write_delta(df_dim_uf, f"{_BASE}/gold/dim_uf/")

    df_dim_contexto = _build_dim_contexto_escolar(spark, df_censo)
    _write_delta(df_dim_contexto, f"{_BASE}/gold/dim_contexto_escolar/")

    # ---- Fact tables -------------------------------------------------------
    df_fato_ind = _build_fato_indicador_alfabetizacao(df_silver)
    rows_clean += df_fato_ind.count()
    _write_delta(
        df_fato_ind,
        f"{_BASE}/gold/fato_indicador_alfabetizacao/",
        partition_by=["ano", "sigla_uf"],
    )

    df_fato_metas = _build_fato_metas_vs_resultado(df_silver)
    if df_fato_metas is not None:
        rows_clean += df_fato_metas.count()
        _write_delta(
            df_fato_metas,
            f"{_BASE}/gold/fato_metas_vs_resultado/",
            partition_by=["ano", "sigla_uf"],
        )

    print(f"[✔] Gold processada: {rows_clean} registros escritos em 6 tabelas Delta.")

    return {
        "rows_input": rows_input,
        "rows_clean": rows_clean,
        "rows_quarantined": 0,
        "quarantine_rate_pct": 0.0,
    }


if __name__ == "__main__":
    process_silver_to_gold()
