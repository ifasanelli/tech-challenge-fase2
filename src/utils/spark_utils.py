import os
from pathlib import Path
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip

_JARS_DIR = Path(__file__).parent.parent.parent / "jars"
_GCS_JAR = str(_JARS_DIR / "gcs-connector-hadoop3-latest.jar")
_DELTA_STORAGE_JAR = str(_JARS_DIR / "delta-storage-3.0.0.jar")


def get_spark_gcs(app_name: str) -> SparkSession:
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    existing_jars = [p for p in [_GCS_JAR, _DELTA_STORAGE_JAR] if Path(p).exists()]

    builder = SparkSession.builder.appName(app_name)

    # Configure Delta Lake with automatic JAR download from Maven
    builder = configure_spark_with_delta_pip(builder)

    builder = (
        builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    if existing_jars:
        builder = builder.config("spark.jars", ",".join(existing_jars))

    if creds and Path(creds).exists():
        builder = (
            builder
            .config("spark.hadoop.fs.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", creds)
            .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
        )

    return builder.getOrCreate()
