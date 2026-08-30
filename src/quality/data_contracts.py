from pyspark.sql import Column
from pyspark.sql.functions import col, length

class DataContracts:
    """Contratos formais de qualidade para os dados de alfabetização."""

    # Limites oficiais (Pesquisa Alfabetiza Brasil / INEP)
    MIN_SAEB_SCORE = 0.0
    MAX_SAEB_SCORE = 1000.0
    MIN_TAXA = 0.0
    MAX_TAXA = 100.0
    IBGE_LENGTH = 7

    @staticmethod
    def rule_completeness(id_col="id_municipio", uf_col="sigla_uf", ano_col="ano") -> Column:
        """Dimensão Completude: Chaves primárias não podem ser nulas."""
        return col(id_col).isNotNull() & col(uf_col).isNotNull() & col(ano_col).isNotNull()

    @staticmethod
    def rule_accuracy(taxa_col="indicador_alfabetizacao", saeb_col="media_proficiencia_saeb") -> Column:
        """Dimensão Acurácia: Taxas percentuais [0, 100] e escala Saeb [0, 1000]."""
        return (
            (col(taxa_col) >= DataContracts.MIN_TAXA) & (col(taxa_col) <= DataContracts.MAX_TAXA) &
            (col(saeb_col) >= DataContracts.MIN_SAEB_SCORE) & (col(saeb_col) <= DataContracts.MAX_SAEB_SCORE)
        )

    @staticmethod
    def rule_validity(id_col="id_municipio") -> Column:
        """Dimensão Validade: Código IBGE municipal deve conter exatamente 7 dígitos."""
        return length(col(id_col)) == DataContracts.IBGE_LENGTH