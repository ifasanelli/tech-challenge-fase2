import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from functools import wraps

logging.basicConfig(level=logging.INFO, format="%(message)s")
_logger = logging.getLogger(__name__)

# Adicionar handler explícito para stderr para garantir captura em testes
_handler = logging.StreamHandler(sys.stderr)
_handler.setLevel(logging.INFO)
_handler.setFormatter(logging.Formatter("%(message)s"))
_logger.addHandler(_handler)

_GCS_BUCKET = os.environ.get("GCS_BUCKET", "tech-challenge-fase2-507116-datalake")


def monitor_stage(stage_name: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            start = time.time()
            metric = {
                "stage": stage_name,
                "batch_id": batch_id,
                "rows_input": None,
                "rows_clean": None,
                "rows_quarantined": None,
                "quarantine_rate_pct": None,
                "latency_seconds": None,
                "status": "RUNNING",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            try:
                result = func(*args, **kwargs)
                metric["status"] = "SUCCESS"
                metric["latency_seconds"] = round(time.time() - start, 2)
                if isinstance(result, dict):
                    metric.update({k: result[k] for k in result if k in metric})
                    _check_quarantine_alert(metric)
                _write_metric(metric, batch_id)
                return result
            except Exception as exc:
                metric["status"] = "ERROR"
                metric["latency_seconds"] = round(time.time() - start, 2)
                metric["error"] = str(exc)
                metric["traceback"] = traceback.format_exc()
                _write_metric(metric, batch_id)
                raise

        return wrapper

    return decorator


def _check_quarantine_alert(metric: dict) -> None:
    rate = metric.get("quarantine_rate_pct")
    if rate is not None and rate > 10.0:
        alert = {**metric, "level": "CRITICAL_ALERT",
                 "message": f"Taxa de quarentena {rate}% excede threshold de 10%"}
        alert_json = json.dumps(alert)
        _logger.warning(alert_json)
        # Garantir que a saída vá para stderr para captura em testes
        print(alert_json, file=sys.stderr)


def _write_metric(metric: dict, batch_id: str) -> None:
    _logger.info(json.dumps(metric))
    try:
        import gcsfs
        date_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = (f"gs://{_GCS_BUCKET}/logs/pipeline/{date_prefix}/"
                f"{metric['stage']}_{batch_id}.jsonl")
        fs = gcsfs.GCSFileSystem()
        with fs.open(path, "w") as fh:
            fh.write(json.dumps(metric) + "\n")
    except Exception:
        pass  # falha no write de log não deve parar o pipeline
