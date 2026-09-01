from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, current_timestamp, lit
from src.quality.data_contracts import DataContracts

class QuarantineManager:
    """Responsável por auditar, rotular e segregar registros que violam contratos."""

    @staticmethod
    def split_clean_and_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
        r_completeness = DataContracts.rule_completeness()
        r_accuracy = DataContracts.rule_accuracy()
        r_validity = DataContracts.rule_validity()

        df_evaluated = df.withColumn(
            "is_conform",
            r_completeness & r_accuracy & r_validity
        ).withColumn(
            "rejection_reason",
            when(~r_completeness, lit("VIOLACAO_COMPLETUDE: Campos obrigatorios nulos"))
            .when(~r_accuracy, lit("VIOLACAO_ACURACIA: Metricas fora da escala permitida"))
            .when(~r_validity, lit("VIOLACAO_VALIDADE: Codigo IBGE com tamanho invalido"))
            .otherwise(lit(None))
        )

        df_clean = df_evaluated.filter(col("is_conform") == True).drop("is_conform", "rejection_reason")

        df_quarantine = (df_evaluated
                         .filter(col("is_conform") == False)
                         .withColumn("_quarantine_timestamp", current_timestamp())
                         .drop("is_conform"))

        return df_clean, df_quarantine