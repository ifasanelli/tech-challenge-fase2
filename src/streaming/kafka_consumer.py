import os
from dotenv import load_dotenv
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from src.utils.spark_utils import get_spark_gcs

load_dotenv()

_KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
_TOPIC = os.environ.get("KAFKA_TOPIC", "educacao.avaliacao.medicao")
_BUCKET = os.environ.get("GCS_BUCKET", "tech-challenge-fase2-507116-datalake")

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("id_municipio", StringType(), False),
    StructField("sigla_uf", StringType(), False),
    StructField("ano", IntegerType(), False),
    StructField("media_proficiencia_saeb", DoubleType(), True),
    StructField("indicador_alfabetizacao", DoubleType(), True),
    StructField("total_alunos_avaliados", IntegerType(), True),
    StructField("timestamp_medicao", StringType(), True),
])


def run_streaming():
    spark = get_spark_gcs("StreamingIngestion-KafkaToBronze")
    spark.sparkContext.setLogLevel("WARN")

    df_raw = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", _KAFKA_BROKER)
              .option("subscribe", _TOPIC)
              .option("startingOffsets", "latest")
              .load())

    df_parsed = (df_raw
                 .selectExpr("CAST(value AS STRING) as json_payload")
                 .select(from_json(col("json_payload"), EVENT_SCHEMA).alias("data"))
                 .select("data.*")
                 .withColumn("_ingestion_timestamp", current_timestamp())
                 .withColumn("_source_topic", lit(_TOPIC)))

    bronze_path = f"gs://{_BUCKET}/bronze/medicoes_stream"
    checkpoint_path = f"gs://{_BUCKET}/checkpoints/bronze_medicoes_stream"

    # Bronze layer uses Parquet (spec: "Bronze: Parquet + Snappy, sem Delta")
    query = (df_parsed.writeStream
             .format("parquet")
             .outputMode("append")
             .option("checkpointLocation", checkpoint_path)
             .start(bronze_path))

    print(f"[*] Streaming ativo → {bronze_path}")
    query.awaitTermination()

if __name__ == "__main__":
    run_streaming()