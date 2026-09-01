#!/usr/bin/env python3
"""Orquestrador sequencial da pipeline completa."""
import logging
import os
import sys
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    log.info("=" * 60)
    log.info("Pipeline Híbrido — Análise da Alfabetização no Brasil")
    log.info("=" * 60)

    log.info("=== Step 1/3: Batch ingestion → Bronze ===")
    from src.ingestion.batch_ingestion import ingest_batch_to_bronze
    result = ingest_batch_to_bronze()
    log.info("Bronze: %s", result)

    log.info("=== Step 2/3: Bronze → Silver ===")
    from src.processing.bronze_to_silver import process_bronze_to_silver
    result = process_bronze_to_silver()
    log.info("Silver: %s", result)

    log.info("=== Step 3/3: Silver → Gold ===")
    from src.processing.silver_to_gold import process_silver_to_gold
    result = process_silver_to_gold()
    log.info("Gold: %s", result)

    _bucket = os.environ.get("GCS_BUCKET", "tech-challenge-fase2-507116-datalake")
    log.info("=== Pipeline complete ===")
    log.info("Logs disponíveis em: gs://%s/logs/pipeline/", _bucket)


if __name__ == "__main__":
    main()
