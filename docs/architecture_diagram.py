import matplotlib.pyplot as plt
import matplotlib.patches as patches

def generate_architecture_diagram():
    fig, ax = plt.subplots(figsize=(16, 9), dpi=300)
    ax.set_facecolor('#F8F9FA')
    fig.patch.set_facecolor('#F8F9FA')

    # Título
    plt.title("Pipeline Híbrido de Dados - Alfabetização Infantil no Brasil\nArquitetura Medalhão Lakehouse com Streaming, Qualidade e FinOps", 
              fontsize=16, fontweight='bold', pad=25, color='#212529')

    # Estilos de Caixas
    box_style = dict(boxstyle="round,pad=0.5", ec="#CED4DA", lw=1.5)

    # 1. Fontes
    ax.text(0.12, 0.78, "FONTES BATCH\nBase dos Dados\n- Metas Alfabetização\n- Censo Escolar INEP", 
            ha="center", va="center", bbox=dict(box_style, facecolor="#E3F2FD", ec="#90CAF9"), fontsize=10, fontweight='bold')
    ax.text(0.12, 0.32, "FONTES STREAMING\nAvaliações Locais\n- Eventos de Medição\n- Revisão de Metas", 
            ha="center", va="center", bbox=dict(box_style, facecolor="#EDE7F6", ec="#B39DDB"), fontsize=10, fontweight='bold')

    # 2. Ingestão
    ax.text(0.32, 0.78, "INGESTÃO BATCH\nCloud Storage (S3/GCS)\nFormat: Parquet / Delta\nSnapshots Históricos", 
            ha="center", va="center", bbox=dict(box_style, facecolor="#BBDEFB"), fontsize=10)
    ax.text(0.32, 0.32, "INGESTÃO REAL-TIME\nApache Kafka (Broker)\nSpark Structured Streaming\nMicro-batches 10s", 
            ha="center", va="center", bbox=dict(box_style, facecolor="#D1C4E9"), fontsize=10)

    # 3. Camada Bronze
    ax.text(0.52, 0.55, "CAMADA BRONZE (RAW)\nDelta Lake / Snappy\n- Preservação Fiel da Origem\n- Metadados de Ingestão\n- Suporte a Time Travel", 
            ha="center", va="center", bbox=dict(box_style, facecolor="#FFE0B2", ec="#FFB74D"), fontsize=10, fontweight='bold')

    # 4. Camada Silver & Quarentena
    ax.text(0.72, 0.68, "CAMADA SILVER\n- Deduplicação por Chave\n- Validação 6 Dimensões\n- Tipagem & Normalização", 
            ha="center", va="center", bbox=dict(box_style, facecolor="#C8E6C9", ec="#81C784"), fontsize=10, fontweight='bold')
    ax.text(0.72, 0.38, "QUARENTENA\n- Isolamento de Falhas\n- Rótulo do Motivo de Erro\n- Auditoria de Qualidade", 
            ha="center", va="center", bbox=dict(box_style, facecolor="#FFCDD2", ec="#E57373"), fontsize=10, fontweight='bold')

    # 5. Camada Gold & Servicing
    ax.text(0.92, 0.78, "CAMADA GOLD (DW)\nStar Schema\n- Fato Alfabetização (Saeb 743)\n- Dimensões Município e UF\n- Partição FinOps (UF/Ano)", 
            ha="center", va="center", bbox=dict(box_style, facecolor="#FFF9C4", ec="#FFF176"), fontsize=10, fontweight='bold')
    ax.text(0.92, 0.32, "PERSISTÊNCIA POLIGLOTA\n- NoSQL JSON (MongoDB)\n- pgvector (RAG Pedagógico)\n- Dashboards & Modelos IA", 
            ha="center", va="center", bbox=dict(box_style, facecolor="#E0F2F1", ec="#80CBC4"), fontsize=10, fontweight='bold')

    # Setas de Conexão
    arrow_props = dict(facecolor='#495057', edgecolor='#495057', arrowstyle="->", lw=2)

    ax.annotate("", xy=(0.22, 0.78), xytext=(0.22, 0.78), arrowprops=arrow_props)
    plt.arrow(0.20, 0.78, 0.04, 0, head_width=0.02, head_length=0.015, fc='#495057', ec='#495057')
    plt.arrow(0.20, 0.32, 0.04, 0, head_width=0.02, head_length=0.015, fc='#495057', ec='#495057')

    plt.arrow(0.40, 0.74, 0.05, -0.12, head_width=0.02, head_length=0.015, fc='#495057', ec='#495057')
    plt.arrow(0.40, 0.36, 0.05, 0.12, head_width=0.02, head_length=0.015, fc='#495057', ec='#495057')

    plt.arrow(0.60, 0.58, 0.05, 0.07, head_width=0.02, head_length=0.015, fc='#495057', ec='#495057')
    plt.arrow(0.60, 0.52, 0.05, -0.07, head_width=0.02, head_length=0.015, fc='#D32F2F', ec='#D32F2F')

    plt.arrow(0.80, 0.68, 0.04, 0.07, head_width=0.02, head_length=0.015, fc='#495057', ec='#495057')
    plt.arrow(0.80, 0.64, 0.04, -0.22, head_width=0.02, head_length=0.015, fc='#495057', ec='#495057')

    ax.set_xlim(0, 1.05)
    ax.set_ylim(0.15, 0.95)
    ax.axis('off')

    plt.tight_layout()
    plt.savefig("docs/architecture_diagram.png", dpi=300)
    plt.close()
    print("[✔] Diagrama arquitetural gerado e salvo em docs/architecture_diagram.png")

if __name__ == "__main__":
    generate_architecture_diagram()