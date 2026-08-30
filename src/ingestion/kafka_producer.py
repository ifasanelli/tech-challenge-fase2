import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuração do Producer Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "educacao.avaliacao.medicao"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Amostra de municípios e UFs para simulação de eventos
MUNICIPIOS_MOCK = [
    {"id_municipio": "3550308", "nome": "São Paulo", "sigla_uf": "SP"},
    {"id_municipio": "3304557", "nome": "Rio de Janeiro", "sigla_uf": "RJ"},
    {"id_municipio": "2304400", "nome": "Fortaleza", "sigla_uf": "CE"},
    {"id_municipio": "2312908", "nome": "Sobral", "sigla_uf": "CE"},
    {"id_municipio": "1302603", "nome": "Manaus", "sigla_uf": "AM"},
]

def gerar_evento_medicao():
    """Gera um evento simulado de avaliação de alfabetização."""
    municipio = random.choice(MUNICIPIOS_MOCK)

    # 5% de chance de gerar dado com erro para testar a quarentena na camada Silver
    induzir_erro = random.random() < 0.05

    if induzir_erro:
        proficiencia = -50.0  # Erro de acurácia
        taxa = 120.0          # Taxa inválida (> 100)
    else:
        # Ponto de corte do Saeb: 743 pontos
        proficiencia = round(random.gauss(735, 35), 2)
        taxa = round(random.uniform(55.0, 98.0), 2)

    return {
        "event_id": f"EVT-{int(time.time() * 1000)}-{random.randint(100, 999)}",
        "id_municipio": municipio["id_municipio"],
        "sigla_uf": municipio["sigla_uf"],
        "ano": 2024,
        "media_proficiencia_saeb": proficiencia,
        "indicador_alfabetizacao": taxa,
        "total_alunos_avaliados": random.randint(150, 2000),
        "timestamp_medicao": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print(f"[*] Iniciando envio de eventos para o tópico: {TOPIC_NAME}")
    try:
        while True:
            evento = gerar_evento_medicao()
            producer.send(TOPIC_NAME, value=evento)
            print(f"[+] Enviado: {evento['event_id']} | Mun: {evento['id_municipio']} | Saeb: {evento['media_proficiencia_saeb']}")
            time.sleep(1.5)
    except KeyboardInterrupt:
        print("[!] Encerramento solicitado pelo usuário.")
    finally:
        producer.flush()
        producer.close()