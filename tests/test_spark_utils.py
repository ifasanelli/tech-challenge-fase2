from src.utils.spark_utils import get_spark_gcs


def test_get_spark_gcs_retorna_sessao_funcional():
    spark = get_spark_gcs("test-spark-utils")
    assert spark is not None
    assert spark.version.startswith("3.")
    df = spark.createDataFrame([("SP", 2023)], ["sigla_uf", "ano"])
    assert df.count() == 1
    spark.stop()
