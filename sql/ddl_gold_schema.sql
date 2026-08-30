-- DDL: Dataset e External Tables BigQuery apontando para GCS Gold
-- Execute via: bq query --use_legacy_sql=false < sql/ddl_gold_schema.sql

CREATE SCHEMA IF NOT EXISTS `tech-challenge-fase2-507116.gold`
OPTIONS (
  description = "Camada Gold — Star Schema do Indicador Criança Alfabetizada",
  location = "us-east1"
);

-- dim_tempo
CREATE OR REPLACE EXTERNAL TABLE `tech-challenge-fase2-507116.gold.dim_tempo`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://tech-challenge-fase2-507116-datalake/gold/dim_tempo/*.parquet']
);

-- dim_uf
CREATE OR REPLACE EXTERNAL TABLE `tech-challenge-fase2-507116.gold.dim_uf`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://tech-challenge-fase2-507116-datalake/gold/dim_uf/*.parquet']
);

-- dim_municipio
CREATE OR REPLACE EXTERNAL TABLE `tech-challenge-fase2-507116.gold.dim_municipio`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://tech-challenge-fase2-507116-datalake/gold/dim_municipio/*.parquet']
);

-- dim_contexto_escolar
CREATE OR REPLACE EXTERNAL TABLE `tech-challenge-fase2-507116.gold.dim_contexto_escolar`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://tech-challenge-fase2-507116-datalake/gold/dim_contexto_escolar/*.parquet']
);

-- fato_indicador_alfabetizacao
CREATE OR REPLACE EXTERNAL TABLE `tech-challenge-fase2-507116.gold.fato_indicador_alfabetizacao`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://tech-challenge-fase2-507116-datalake/gold/fato_indicador_alfabetizacao/*/*.parquet'],
  hive_partition_uri_prefix = 'gs://tech-challenge-fase2-507116-datalake/gold/fato_indicador_alfabetizacao'
);

-- fato_metas_vs_resultado
CREATE OR REPLACE EXTERNAL TABLE `tech-challenge-fase2-507116.gold.fato_metas_vs_resultado`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://tech-challenge-fase2-507116-datalake/gold/fato_metas_vs_resultado/*/*.parquet'],
  hive_partition_uri_prefix = 'gs://tech-challenge-fase2-507116-datalake/gold/fato_metas_vs_resultado'
);
