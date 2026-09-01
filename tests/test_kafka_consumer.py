import os
import pytest
from unittest.mock import patch


def test_env_config_defaults():
    """Test that path constants resolve correctly with default env vars"""
    # Import after patching to ensure defaults are used
    with patch.dict(os.environ, {}, clear=False):
        # Remove any existing env vars
        os.environ.pop("GCS_BUCKET", None)
        os.environ.pop("KAFKA_BROKER", None)
        os.environ.pop("KAFKA_TOPIC", None)

        # Import the module
        import importlib
        from src.streaming import kafka_consumer
        importlib.reload(kafka_consumer)

        assert kafka_consumer._BUCKET == "tech-challenge-fase2-507116-datalake"
        assert kafka_consumer._KAFKA_BROKER == "localhost:9092"
        assert kafka_consumer._TOPIC == "educacao.avaliacao.medicao"


def test_gcs_paths_construction():
    """Test that GCS paths are constructed correctly"""
    with patch.dict(os.environ, {"GCS_BUCKET": "test-bucket"}, clear=False):
        import importlib
        from src.streaming import kafka_consumer
        importlib.reload(kafka_consumer)

        bronze_path = f"gs://{kafka_consumer._BUCKET}/bronze/medicoes_stream"
        checkpoint_path = f"gs://{kafka_consumer._BUCKET}/checkpoints/bronze_medicoes_stream"

        assert bronze_path == "gs://test-bucket/bronze/medicoes_stream"
        assert checkpoint_path == "gs://test-bucket/checkpoints/bronze_medicoes_stream"


def test_custom_env_vars():
    """Test that custom env vars override defaults"""
    custom_env = {
        "GCS_BUCKET": "custom-datalake",
        "KAFKA_BROKER": "kafka.example.com:9092",
        "KAFKA_TOPIC": "custom.topic"
    }
    with patch.dict(os.environ, custom_env, clear=False):
        import importlib
        from src.streaming import kafka_consumer
        importlib.reload(kafka_consumer)

        assert kafka_consumer._BUCKET == "custom-datalake"
        assert kafka_consumer._KAFKA_BROKER == "kafka.example.com:9092"
        assert kafka_consumer._TOPIC == "custom.topic"


def test_event_schema_exists():
    """Test that EVENT_SCHEMA is properly defined"""
    from src.streaming.kafka_consumer import EVENT_SCHEMA

    assert EVENT_SCHEMA is not None
    assert len(EVENT_SCHEMA.fields) == 8

    field_names = [f.name for f in EVENT_SCHEMA.fields]
    assert "event_id" in field_names
    assert "id_municipio" in field_names
    assert "sigla_uf" in field_names
    assert "ano" in field_names
    assert "media_proficiencia_saeb" in field_names
    assert "indicador_alfabetizacao" in field_names
    assert "total_alunos_avaliados" in field_names
    assert "timestamp_medicao" in field_names
