# 📊 Pipeline Híbrido para Análise da Alfabetização no Brasil
### Pós-Tech AI Scientist — FIAP | Tech Challenge — Fase 2

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-00ADD8?style=flat)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.6-231F20?style=flat&logo=apachekafka&logoColor=white)
![Cloud Data](https://img.shields.io/badge/Cloud-AWS_%7C_GCP-orange?style=flat&logo=amazonwebservices&logoColor=white)
![FinOps](https://img.shields.io/badge/FinOps-Cost_Optimized-success?style=flat)

---

## 📌 1. Contexto do Problema & Desafio Educacional

O **Compromisso Nacional Criança Alfabetizada (Decreto nº 11.556/2023)** estabelece a meta de garantir que **100% das crianças brasileiras estejam alfabetizadas ao final do 2º ano do ensino fundamental até 2030**, além de recompor as aprendizagens afetadas pela pandemia.

O parâmetro técnico de proficiência foi definido pela **Pesquisa Alfabetiza Brasil (INEP, 2023)**, que fixou a nota de corte em **743 pontos na escala do Saeb**. Alunos com pontuação igual ou superior a 743 são considerados alfabetizados, gerando o **Indicador Criança Alfabetizada**.

### Dores Estruturais Enfrentadas pela Gestão Pública:
1. **Dados Fragmentados e Descentralizados**: Indicadores de metas, dados censitários e microdados de proficiência residem em formatos díspares (tabelas analíticas da Base dos Dados, APIs públicas e registros administrativos).
2. **Latência no Monitoramento**: Avaliações diagnósticas e atualizações de metas municipais dependiam historicamente de consolidações anuais estáticas, inviabilizando ações corretivas ágeis durante o ano letivo.
3. **Falta de Governança e Rastreabilidade**: Inconsistências de códigos IBGE, duplicidades e ausência de histórico de alterações comprometem a confiabilidade de alocações orçamentárias (como complementações do FUNDEB).

---

## 🏛️ 2. Arquitetura da Solução

A solução adota uma **Arquitetura Híbrida (Batch + Streaming)** baseada no paradigma **Lakehouse** com **Arquitetura Medalhão (Bronze, Silver, Gold)**, implementando **Data Mesh** e separação total entre **Computação e Armazenamento**.

### Diagrama Arquitetural de Alto Nível

```
                                  FONTES DE DADOS
  ┌───────────────────────────────────────┐   ┌───────────────────────────────────┐
  │         Base dos Dados (Batch)        │   │    Sistemas Locais / Escolas      │
  │ Metas Brasil/UF/Município, Território │   │ Eventos de Medições e Avaliações  │
  └──────────────────┬────────────────────┘   └─────────────────┬─────────────────┘
                     │ Ingestão Batch                           │ CDC / Eventos
                     ▼                                          ▼
            ┌──────────────────┐                     ┌─────────────────────┐
            │ Cloud Storage /  │                     │    Apache Kafka     │
            │ Ingestão Raw     │                     │ (Broker Multi-AZ)   │
            └────────┬─────────┘                     └──────────┬──────────┘
                     │                                          │ Spark Streaming
                     └────────────────────┬─────────────────────┘
                                          ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        CAMADA BRONZE (Data Lakehouse / S3 - GCS)                       │
│ - Armazenamento de dados brutos sem transformação de negócio                           │
│ - Formato aberto Delta Lake / Parquet com compressão Snappy                            │
│ - Metadados de linhagem anexados: _ingestion_timestamp, _source_file, _batch_id        │
└─────────────────────────────────────────┬──────────────────────────────────────────────┘
                                          │
                                          ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                        CAMADA SILVER (Tratamento & Qualidade)                             │
│ - Deduplicação por chave composta (id_municipio, ano, id_aluno)                           │
│ - Tipagem estrita e normalização de identificadores (IBGE 7 dígitos)                      │
│ - Validação automática das 6 Dimensões de Qualidade de Dados (PySpark + Data Contracts)   │
│                                                                                           │
│   ┌────────────────────────────────────┐       ┌───────────────────────────────────┐      │
│   │   Tabelas Conformes (Silver Delta) │       │ Quarentena (silver_quarantine)    │      │
│   │   Dados validados para modelagem   │       │ Registros inválidos com motivo    │      │
│   └─────────────────┬──────────────────┘       └───────────────────────────────────┘      │
└─────────────────────┼─────────────────────────────────────────────────────────────────────┘
                      │
                      ├────────────────────────────────────────┬─────────────────────────┐
                      ▼                                        ▼                         ▼
┌──────────────────────────────────────────┐   ┌────────────────────────────┐   ┌──────────────────────────┐
│        CAMADA GOLD (Star Schema)         │   │      NoSQL & CACHE         │   │    BANCO VETORIAL        │
│ - Tabelas Dimensões e Fato               │   │ - Documentos JSON flexíveis│   │ - Embeddings de Planos   │
│   (Fato Indicador, Fato Metas)           │   │   (Censo Escolar)          │   │   Municipais e Parecer   │
│ - Agregações em nível UF e Município     │   │ - Redis / Key-Value Cache  │   │   Técnico Pedagógico     │
│ - Pronto para SQL, BI e Modelos de ML    │   │   para APIs de latência ms │   │ - RAG para IA e Políticas│
└─────────────────────┬────────────────────┘   └────────────────────────────┘   └──────────────────────────┘
                      ▼
         CONSUMO, ANALYTICS & INSIGHTS
   [ Dashboards BI | Consultas SQL | Modelos Preditivos de Regressão/Classificação ]
```

---

## 📂 3. Fontes de Dados & Princípios de Data Mesh

O projeto integra as seguintes entidades disponibilizadas na **Base dos Dados**:
* **`br_inep_alfabetizacao.uf`**: Indicadores e metas agregados em nível estadual.
* **`br_inep_alfabetizacao.municipio`**: Indicadores e metas oficiais de cada município.
* **`br_inep_alfabetizacao.brasil`**: Consolidação histórica da meta nacional.
* **`br_inep_censo_escolar.aluno`**: Microdados amostrais/contextuais do corpo discente.
* **Fontes Contextuais Complementares**: Infraestrutura escolar (Censo Escolar/INEP), PIB e população municipal (IBGE), repasses do FUNDEB.

### Aplicação dos 4 Pilares de Data Mesh:
1. **Domain-Driven Ownership**: O pipeline é decomposto por domínios de negócio:
   * *Domínio Metas*: Responsável pelos dados de pactuação e cumprimento do Saeb 743.
   * *Domínio Território*: Responsável pelos metadados geográficos de municípios e UFs.
   * *Domínio Escolas & Alunos*: Responsável pelos microdados contextuais do Censo.
2. **Data as a Product**: Cada tabela da camada Gold possui um contrato de schema (`schema_contract.json`), documentação descritiva de cada campo, SLAs de atualização e testes de qualidade.
3. **Self-Serve Data Platform**: Infraestrutura baseada em armazenamento de objetos em nuvem e clusters efêmeros de processamento gerenciado (Apache Spark / Databricks), disponibilizando catálogos para consumo autônomo.
4. **Governança Computacional Federada**: Políticas globais de segurança, auditoria e testes de integridade aplicados como código antes da promoção para as camadas públicas.

---

## 🥇 4. Camadas Medalhão & Mecanismo de Quarentena

| Camada | Formato de Armazenamento | Propósito Técnico | Garantias e Metadados |
| :--- | :--- | :--- | :--- |
| **Bronze (Raw)** | Delta Lake / Parquet | Preservação fiel dos dados de origem, sem descarte de registros | Inclusão de `_ingestion_timestamp`, `_source_topic` ou `_file_name`. Permite reprocessamento total (*Time Travel*). |
| **Silver (Cleansed)** | Delta Lake | Limpeza, conversão de tipos primitivos, deduplicação e junção entre domínios | Aplicação de regras de integridade relacional e particionamento físico por `sigla_uf`. |
| **Gold (Curated)** | Delta Lake / Star Schema | Modelagem dimensional e cálculo de KPIs analíticos agregados | Esquema Estrela estruturado para consultas corporativas de alta performance e consumo por modelos de Machine Learning. |

### Fluxo de Quarentena de Dados
Para evitar que dados corrompidos invadam a camada Gold sem perder rastreabilidade operacional:
1. Durante o processamento da Silver, cada linha é avaliada contra uma matriz de regras de validação.
2. Registros que violam regras críticas (ex: `taxa_alfabetizacao < 0` ou `taxa_alfabetizacao > 100`, código IBGE nulo ou com comprimento inválido) são segregados.
3. Esses registros são gravados na tabela `silver_quarantine`, acompanhados das colunas `rejection_reason` e `quarantine_timestamp`.
4. Os registros válidos seguem o fluxo convencional para gravação nas tabelas limpas da Silver.

---

## ⚡ 5. Ingestão Híbrida: Batch vs. Streaming

| Característica | Ingestão Batch | Ingestão Streaming (Apache Kafka) |
| :--- | :--- | :--- |
| **Tecnologia** | PySpark + Cloud Storage (S3/GCS) | Apache Kafka + Spark Structured Streaming |
| **Gatilho / Frequência** | Agendamento diário / semanal (Airflow) | Contínuo baseado em eventos (micro-batches de 10s) |
| **Casos de Uso** | Metas Nacionais, Censo Escolar, Censo IBGE | Lançamentos de avaliações diagnósticas, revisão de metas |
| **Semântica** | Idempotente via `overwrite` particionado / `merge` | **At-Least-Once** com deduplicação idempotente na Silver |
| **Formato de Serialização**| CSV / Parquet | JSON com schema ou Avro registrado |

---

## 🗄️ 6. Persistência Poliglota: Relacional, NoSQL e Banco Vetorial

Em conformidade com as demandas contemporâneas de IA e engenharia de dados, adotamos **Persistência Poliglota**:

1. **Relacional / Data Warehouse (Camada Gold)**:
   * Implementação de **Star Schema** com tabela Fato (`fato_indicador_alfabetizacao`) e Dimensões (`dim_municipio`, `dim_uf`, `dim_tempo`, `dim_contexto_escolar`).
   * Consultas analíticas avançadas com **CTEs (Common Table Expressions)** e funções de janela (`RANK()`, `AVG() OVER()`).
2. **NoSQL Documental (JSON Flexível)**:
   * Armazenamento das fichas de infraestrutura escolar do Censo, contendo centenas de atributos esparsos (laboratórios, acessibilidade, saneamento, internet) em documentos JSON hierárquicos, dispensando dezenas de `JOINs` relacionais custosos.
3. **NoSQL Chave-Valor (Redis / DynamoDB)**:
   * Cache de leitura instantânea para portais públicos e secretarias municipais:
     * Chave: `kpi:municipio:{cod_ibge}:ano:{ano}` $\rightarrow$ Valor: `{"taxa": 78.4, "meta": 80.0, "status": "ALERTA"}`.
4. **Banco Vetorial (Pinecone / ChromaDB / pgvector)**:
   * Vetorização dos planos municipais de educação e pareceres pedagógicos do INEP via modelos de *text-embedding*.
   * Permite **busca semântica** e arquitetura **RAG (Retrieval-Augmented Generation)**: quando um gestor de um município com baixa proficiência consulta a plataforma, o sistema recupera os planos de ação pedagógica de municípios com perfil socioeconômico equivalente que conseguiram superar a meta de 743 pontos.

---

## 🛡️ 7. Governança e Qualidade de Dados (6 Dimensões)

O pipeline executa testes automatizados baseados nas dimensões formais de Data Quality:
* **Completude**: Garantir 0% de nulos nas chaves de particionamento e IDs (`id_municipio`, `sigla_uf`, `ano`).
* **Acurácia (Precisão)**: O `indicador_alfabetizacao` deve estar estritamente contido no intervalo decimal entre `0.0` e `100.0`, e a nota Saeb entre `0` e `1000`.
* **Consistência**: O código do município (`id_municipio`) deve possuir 7 dígitos válidos e seu prefixo de 2 dígitos deve coincidir com o código da UF correspondente.
* **Validade**: Validação de schemas contra contratos formais definidos em JSON Schema / Delta Constraints.
* **Pontualidade**: Aferição da defasagem temporal entre a data do evento da medição e sua gravação na camada analítica.
* **Unicidade**: Aplicação de restrição de unicidade para a chave primária composta `(id_municipio, ano)`.

---

## 💰 8. FinOps & Eficiência em Cloud

A infraestrutura foi desenhada sob as premissas do framework **FinOps (Inform, Optimize, Operate)**:

1. **Separação de Computação e Armazenamento**:
   * O armazenamento é centralizado em Object Storage durável de baixo custo (Amazon S3 / Google Cloud Storage), enquanto o processamento computacional ocorre por meio de clusters efêmeros que são desligados imediatamente após a execução do job.
2. **Formato Colunar e Columnar Pruning**:
   * O uso de arquivos Parquet/Delta com compressão Snappy reduz o tamanho de armazenamento em até 75% comparado ao CSV, além de permitir leitura seletiva de colunas nas consultas analíticas.
3. **Estratégia de Particionamento Físico**:
   * Tabelas Silver e Gold particionadas fisicamente por `sigla_uf` e `ano`. Consultas analíticas filtradas por estado evitam o escaneamento integral da base (*Full Table Scan*), reduzindo custos de consultas em ferramentas serverless como BigQuery ou Athena.
4. **Políticas de Ciclo de Vida (Data Tiering)**:
   * Regras automáticas de *Lifecycle Management* no bucket: dados da camada Bronze migram para *S3 Standard-Infrequent Access* após 60 dias e para *Glacier Flexible Retrieval* após 180 dias.
5. **Tagging Mandatório para Alocação de Custos**:
   * Todos os recursos de nuvem contêm tags padronizadas: `Project=TechChallengeFase2`, `Environment=Production`, `Domain=Education`, `CostCenter=1042`.
6. **Métricas de Eficiência**:
   * Acompanhamento do KPI de *Unit Economics*: Custo de Nuvem por Milhão de Registros Processados ($/1M rows).

---

## ⚖️ 9. Decisões Arquiteturais e Trade-Offs

| Decisão Arquitetural | Alternativa Avaliada | Escolha Adotada | Justificativa Técnica & Trade-Off |
| :--- | :--- | :--- | :--- |
| **Paradigma de Armazenamento** | Data Lake Puro vs. Data Warehouse Rígido | **Lakehouse (Delta Lake)** | Une o baixo custo do storage distribuído com confiabilidade ACID, evitando duplicação de dados entre Lake e DW. |
| **Ingestão de Dados** | Batch Exclusivo vs. Streaming Exclusivo | **Arquitetura Híbrida** | Batch para cargas massivas históricas (baixo custo de processamento agendado); Streaming para eventos de atualização de metas e medições com baixa latência. |
| **Motor de Processamento** | Pandas em VM vs. Apache Spark | **Apache Spark (PySpark)** | O Pandas opera limitado à memória RAM de um único nó. O Spark distribui o processamento em cluster, suportando o volume nacional de microdados com tolerância a falhas. |
| **Processamento Streaming** | Exactly-Once no Kafka vs. At-Least-Once com Deduplicação | **At-Least-Once + Idempotência** | Configurar transações *Exactly-Once* completas no broker gera sobrecarga de latência e custo computacional. A ingestão *At-Least-Once* combinada com deduplicação via `MERGE` na Silver garante consistência com menor custo. |

---

## 🤖 10. Aplicações em Inteligência Artificial & Políticas Públicas

A base tratada na camada **Gold** fornece os dados prontos para alimentar modelos avançados de IA:

1. **Modelos Preditivos de Atingimento da Meta (Classificação/Regressão)**:
   * Algoritmos como LightGBM e XGBoost treinados sobre a base Gold histórica para prever a probabilidade de um município não atingir a nota de corte (743 pontos) em 2030, permitindo intervenção preventiva das secretarias de educação.
2. **Clustering para Identificação de Vulnerabilidade Educacional**:
   * Algoritmos de agrupamento não-supervisionado (K-Means / HDBSCAN) combinando proficiência, infraestrutura escolar e dados de repasse orçamentário para agrupar municípios com perfis de risco similares.
3. **Alocação Inteligente de Recursos Orçamentários**:
   * Otimização de redistribuição de repasses do FUNDEB com base nas lacunas de aprendizado diagnosticadas pelo pipeline.
4. **Agente Pedagógico RAG (GenAI)**:
   * Conexão da camada analítica Gold e do Banco Vetorial a Large Language Models (LLMs) para apoiar secretários de educação na redação de planos de contingência técnica baseados em evidências.

---

## 📁 11. Estrutura do Repositório

```bash
├── README.md                        # Documentação técnica completa
├── docker-compose.yml               # Subida do ecossistema local (Kafka, Zookeeper, Spark)
├── requirements.txt                 # Dependências Python
├── pytest.ini                       # Configuração do pytest
├── run_pipeline.py                  # Orquestrador sequencial da pipeline completa (GCP)
├── run_local_demo.py                # Demo local sem GCP/Java: Bronze → Silver → Gold com pandas
├── src/
│   ├── ingestion/
│   │   ├── batch_ingestion.py       # Coleta dos datasets da Base dos Dados -> Bronze
│   │   └── kafka_producer.py        # Produtor simulador de eventos de medição Saeb
│   ├── processing/
│   │   ├── bronze_to_silver.py      # Pipeline PySpark: limpeza, normalização e quarentena
│   │   └── silver_to_gold.py        # Pipeline PySpark: modelagem dimensional (Fato e Dimensões)
│   ├── streaming/
│   │   └── kafka_consumer.py        # Spark Structured Streaming consumindo Kafka -> Silver
│   ├── quality/
│   │   ├── data_contracts.py        # Regras de validação e contrato de schema
│   │   └── quarantine_manager.py    # Segregação de registros inconsistentes para quarentena
│   ├── monitoring/
│   │   └── pipeline_monitor.py      # Monitoramento e métricas de execução da pipeline
│   ├── utils/
│   │   └── spark_utils.py           # Utilitários compartilhados para sessões Spark/Delta
│   └── nosql/
│       ├── nosql_document_loader.py # Carregamento de dados desnormalizados no MongoDB
│       └── vector_indexing.py       # Geração de embeddings e indexação vetorial
├── sql/
│   ├── ddl_gold_schema.sql          # Criação das tabelas Star Schema
│   └── analytical_queries.sql       # Queries analíticas com CTEs e funções de janela
├── data/
│   └── mock/                        # Dados de amostra locais para testes sem billing GCP
│       ├── br_inep_alfabetizacao_brasil.csv
│       ├── br_inep_alfabetizacao_municipio.csv
│       ├── br_inep_alfabetizacao_uf.csv
│       ├── br_inep_censo_escolar_aluno.csv
│       ├── ibge_municipios.csv
│       └── ibge_ufs.csv
├── tests/
│   ├── conftest.py                  # Fixtures compartilhadas do pytest
│   ├── test_batch_ingestion.py      # Testes da ingestão batch
│   ├── test_kafka_consumer.py       # Testes do consumidor Kafka/Streaming
│   ├── test_monitoring.py           # Testes do módulo de monitoramento
│   ├── test_quality_rules.py        # Testes das regras de qualidade de dados
│   ├── test_silver_to_gold.py       # Testes da transformação Silver → Gold
│   └── test_spark_utils.py          # Testes dos utilitários Spark
└── docs/
    ├── infra_setup.sh               # Script de provisionamento da infraestrutura GCP
    ├── architecture_diagram.png     # Diagrama visual da arquitetura
    ├── architecture_diagram.py      # Script gerador do diagrama arquitetural
    └── resultado_query1_simulado.md # Resultado simulado da query analítica principal
```

---

## Infraestrutura GCP

**Bucket GCS:** `gs://tech-challenge-fase2-507116-datalake/`

| Camada | Prefixo GCS | Formato |
|--------|-------------|---------|
| Bronze | `bronze/` | Parquet + Snappy |
| Silver | `silver/` | Delta Lake |
| Gold | `gold/` | Delta Lake (External Tables no BigQuery) |
| Logs | `logs/pipeline/` | JSON Lines |

**Custo estimado mensal:** < USD 2,00 (GCS ~50GB + BigQuery free tier 1TB/mês)

Para provisionar a infraestrutura:
```bash
bash docs/infra_setup.sh
```

Para executar o pipeline completo:
```bash
python run_pipeline.py
```

---

## 🚀 12. Guia de Execução

### Pré-requisitos
* Python 3.10 ou superior
* Docker e Docker Compose instalados
* Ambiente com acesso à internet ou credenciais cloud (AWS / GCP) configuradas

### Setup da Infraestrutura GCP (executar uma única vez)

1. Configure suas credenciais GCP:
   ```bash
   # Aponte a variável de ambiente para o arquivo de service account:
   export GOOGLE_APPLICATION_CREDENTIALS="credentials/service-account.json"
   ```

2. Execute o script de infraestrutura (cria bucket, lifecycle rules, JARs, BigQuery):
   ```bash
   bash docs/infra_setup.sh
   ```

3. Autentique o Base dos Dados (abre link no navegador na primeira execução):
   ```bash
   python -c "import basedosdados as bd; bd.read_sql('SELECT 1', billing_project_id='tech-challenge-fase2-507116')"
   ```

### Executar a Pipeline Completa

```bash
python run_pipeline.py
```

### Estimativa de Custo GCP (mensal, volume do projeto)

| Serviço | Estimativa |
|---|---|
| GCS Standard (~50 GB) | ~$1,00/mês |
| GCS Nearline (Bronze após 60d) | ~$0,20/mês |
| BigQuery queries (<1 TB/mês) | $0 (free tier) |
| Base dos Dados reads (<1 TB/mês) | $0 (free tier) |
| **Total estimado** | **< $2,00/mês** |

### Passo a Passo

1. **Clonar o repositório:**
   ```bash
   git clone https://github.com/usuario/tech-challenge-fase2.git
   cd tech-challenge-fase2
   ```

2. **Criar e ativar o ambiente virtual:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # No Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Subir os serviços de infraestrutura local (Kafka, Spark):**
   ```bash
   docker-compose up -d
   ```

4. **Executar a Ingestão Batch (Base dos Dados $\rightarrow$ Bronze):**
   ```bash
   python src/ingestion/batch_ingestion.py
   ```

5. **Iniciar a Simulação de Streaming:**
   ```bash
   # Terminal 1: Iniciar o consumidor Spark Streaming
   python src/streaming/kafka_consumer.py

   # Terminal 2: Iniciar o gerador de eventos de medição
   python src/ingestion/kafka_producer.py
   ```

6. **Processar as Camadas Medalhão (Bronze $\rightarrow$ Silver $\rightarrow$ Gold):**
   ```bash
   python src/processing/bronze_to_silver.py
   python src/processing/silver_to_gold.py
   ```

7. **Executar as Validações e Testes de Qualidade:**
   ```bash
   pytest tests/
   ```

---

## 🌿 13. Padrões de Git & Versionamento

O repositório adota o modelo **Git Flow**:
* **`main`**: Código estável e validado, pronto para produção.
* **`feature/*`**: Branches isoladas criadas para componentes específicos (`feature/bronze-ingestion`, `feature/kafka-streaming`, `feature/gold-dimensional`).

### Padrão de Commits Semânticos
Os commits seguem a convenção *Conventional Commits*:
* `feat(bronze)`: Adiciona script de extração automática da Base dos Dados
* `feat(silver)`: Implementa pipeline de validação de qualidade e isolamento de quarentena
* `feat(streaming)`: Configura tópico Kafka e consumidor Spark Streaming
* `perf(gold)`: Aplica particionamento físico por UF na tabela fato
* `docs(readme)`: Atualiza diagrama arquitetural e especificações FinOps

---

## 👥 14. Integrante do Grupo
* **Italo Fasanelli Leomil** — RM: rm374889
```