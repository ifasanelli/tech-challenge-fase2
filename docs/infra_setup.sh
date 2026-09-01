#!/usr/bin/env bash
# Setup da infraestrutura GCP para o pipeline de alfabetização
# Pré-requisitos: gcloud CLI autenticado, gsutil disponível, bq disponível
# Uso: bash docs/infra_setup.sh

set -euo pipefail

PROJECT_ID="tech-challenge-fase2-507116"
BUCKET="tech-challenge-fase2-507116-datalake"
REGION="southamerica-east1"

echo "=== [1/5] Criando bucket GCS ==="
gsutil mb -p "$PROJECT_ID" -l "$REGION" -b on "gs://$BUCKET" || echo "Bucket já existe."

echo "=== [2/5] Configurando Lifecycle Rules (Bronze tiering) ==="
cat > /tmp/lifecycle.json << 'EOF'
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
        "condition": {"age": 60, "matchesPrefix": ["bronze/"]}
      },
      {
        "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
        "condition": {"age": 180, "matchesPrefix": ["bronze/"]}
      }
    ]
  }
}
EOF
gsutil lifecycle set /tmp/lifecycle.json "gs://$BUCKET"

echo "=== [3/5] Baixando GCS Connector JAR para Spark ==="
mkdir -p jars
curl -fLo jars/gcs-connector-hadoop3-latest.jar \
  "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.19.jar"
curl -fLo jars/delta-storage-3.0.0.jar \
  "https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar"
echo "JARs baixados em ./jars/"

echo "=== [4/5] Criando dataset BigQuery Gold ==="
bq mk --location="$REGION" --dataset "$PROJECT_ID:gold" || echo "Dataset já existe."

echo "=== [5/5] Criando External Tables BigQuery ==="
bq query --use_legacy_sql=false < sql/ddl_gold_schema.sql

echo ""
echo "=== Setup concluído! ==="
echo "Próximo passo: copie credentials/service-account.json e configure .env"
