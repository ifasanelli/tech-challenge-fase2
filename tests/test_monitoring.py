import pytest
from src.monitoring.pipeline_monitor import monitor_stage


def test_monitor_stage_preserva_retorno():
    @monitor_stage("test_sucesso")
    def minha_funcao():
        return {"rows_input": 100, "rows_clean": 95, "rows_quarantined": 5,
                "quarantine_rate_pct": 5.0}

    resultado = minha_funcao()
    assert resultado["rows_input"] == 100
    assert resultado["rows_clean"] == 95


def test_monitor_stage_relanca_excecao():
    @monitor_stage("test_erro")
    def funcao_com_erro():
        raise ValueError("erro simulado")

    with pytest.raises(ValueError, match="erro simulado"):
        funcao_com_erro()


def test_monitor_stage_alerta_quarentena_alta(capsys):
    @monitor_stage("test_alerta")
    def funcao_quarentena_alta():
        return {"rows_input": 100, "rows_clean": 80, "rows_quarantined": 20,
                "quarantine_rate_pct": 20.0}

    funcao_quarentena_alta()
    captured = capsys.readouterr()
    assert "CRITICAL_ALERT" in captured.out or "CRITICAL_ALERT" in captured.err
