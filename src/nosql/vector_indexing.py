import psycopg2
from pgvector.psycopg2 import register_vector
import numpy as np

DB_URL = "postgresql://postgres:postgrespassword@localhost:5432/educacao_db"

def init_vector_database():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Habilita a extensão pgvector
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    # Criação da tabela para RAG de planos municipais
    cur.execute("""
        CREATE TABLE IF NOT EXISTS plano_pedagogico_vetorial (
            id SERIAL PRIMARY KEY,
            id_municipio VARCHAR(7),
            sigla_uf VARCHAR(2),
            titulo_iniciativa TEXT,
            conteudo_diretriz TEXT,
            embedding vector(4) -- Dimensão mock para demonstração
        );
    """)
    conn.commit()

    register_vector(conn)

    # Inserção de planos de intervenção vetorizados
    planos = [
        (
            "2312908", "CE",
            "Avaliação Diagnóstica Contínua",
            "Monitoramento semanal de fluência leitora e intervenções personalizadas para alunos abaixo do corte de 743.",
            np.array([0.92, 0.15, 0.35, 0.40])
        ),
        (
            "3550308", "SP",
            "Reforço Escolar no Contraturno",
            "Aulas extras de recomposição de leitura e escrita com material didático estruturado.",
            np.array([0.78, 0.25, 0.40, 0.30])
        ),
        (
            "1302603", "AM",
            "Formação de Tutores Itinerantes",
            "Envio de coordenadores pedagógicos em barcos-escola para capacitação docente ribeirinha.",
            np.array([0.65, 0.70, 0.20, 0.15])
        )
    ]

    for p in planos:
        cur.execute("""
            INSERT INTO plano_pedagogico_vetorial (id_municipio, sigla_uf, titulo_iniciativa, conteudo_diretriz, embedding)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, p)

    conn.commit()
    print("[✔] Banco Vetorial: Planos pedagógicos indexados com sucesso via pgvector.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    init_vector_database()