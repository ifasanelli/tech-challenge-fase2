from pymongo import MongoClient

def get_mongo_collection():
    client = MongoClient("mongodb://admin:adminpassword@localhost:27017/")
    db = client["educacao_nosql"]
    return db["censo_infraestrutura_escolas"]

def load_escolas_contexto():
    col = get_mongo_collection()

    # Amostra de documentos NoSQL (atributos esparsos de infraestrutura escolar)
    escolas_docs = [
        {
            "id_escola": "ESC-CE-001",
            "id_municipio": "2312908",
            "nome_escola": "Escola Municipal Maria do Carmo",
            "localizacao": "Urbana",
            "infraestrutura": {
                "agua_potavel": True,
                "energia_rede": True,
                "esgoto_sanitario": True,
                "laboratorio_informatica": True,
                "internet_banda_larga": True,
                "velocidade_mbps": 100
            },
            "recursos_pedagogicos": ["Biblioteca", "Sala de Leitura", "Kits Alfabetiza Brasil"],
            "total_turmas_2ano": 4
        },
        {
            "id_escola": "ESC-AM-002",
            "id_municipio": "1302603",
            "nome_escola": "Escola Ribeirinha Rio Negro",
            "localizacao": "Rural / Ribeirinha",
            "infraestrutura": {
                "agua_potavel": True,
                "energia_rede": False,
                "energia_solar": True,
                "esgoto_sanitario": False,
                "internet_banda_larga": True,
                "conexao_satelital": True
            },
            "recursos_pedagogicos": ["Cantinho da Leitura"],
            "total_turmas_2ano": 1
        }
    ]

    for doc in escolas_docs:
        col.update_one({"id_escola": doc["id_escola"]}, {"$set": doc}, upsert=True)

    print(f"[✔] NoSQL: {len(escolas_docs)} documentos de infraestrutura escolar persistidos no MongoDB.")

if __name__ == "__main__":
    load_escolas_contexto()