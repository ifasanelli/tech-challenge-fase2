-- =====================================================================
-- Query 1: Ranking de Estados por Gap Médio do Saeb em Relação ao Corte 743
-- =====================================================================
-- Regra de Negócio: Corte de 743 pontos no Saeb (Alfabetiza Brasil)

WITH ranking_estados AS (
    -- Agrupamento e agregação em nível de Unidade Federativa
    SELECT
        sigla_uf,
        COUNT(id_municipio) AS total_municipios,
        ROUND(AVG(media_proficiencia_saeb), 2) AS media_saeb_uf,
        ROUND(AVG(indicador_alfabetizacao), 2) AS taxa_media_alfabetizada,
        SUM(CASE WHEN atingiu_corte_alfabetizado = TRUE THEN 1 ELSE 0 END) AS municipios_na_meta,
        ROUND(AVG(media_proficiencia_saeb) - 743.0, 2) AS gap_media_saeb
    FROM `tech-challenge-fase2-507116`.gold.fato_indicador_alfabetizacao
    GROUP BY sigla_uf
),
gap_analise AS (
    -- Cálculo do percentual de cumprimento e status
    SELECT
        sigla_uf,
        total_municipios,
        media_saeb_uf,
        taxa_media_alfabetizada,
        municipios_na_meta,
        ROUND((municipios_na_meta * 100.0 / total_municipios), 2) AS perc_municipios_conformes,
        gap_media_saeb
    FROM ranking_estados
)
SELECT
    sigla_uf,
    total_municipios,
    media_saeb_uf,
    taxa_media_alfabetizada,
    municipios_na_meta,
    perc_municipios_conformes,
    gap_media_saeb,
    CASE
        WHEN perc_municipios_conformes >= 80.0 THEN 'Prioridade Baixa'
        WHEN perc_municipios_conformes >= 50.0 THEN 'Prioridade Media'
        ELSE 'Prioridade Alta (Intervencao Urgente)'
    END AS classificacao_politica_publica
FROM gap_analise
ORDER BY perc_municipios_conformes ASC, media_saeb_uf ASC;

-- =====================================================================
-- Query 2: Evolução Temporal do Indicador por UF (Variação YoY)
-- =====================================================================
WITH evolucao AS (
    SELECT
        sigla_uf,
        ano,
        ROUND(AVG(media_proficiencia_saeb), 2)      AS media_saeb_uf,
        ROUND(AVG(indicador_alfabetizacao), 2)       AS taxa_media_uf,
        SUM(CAST(atingiu_corte_alfabetizado AS INT)) AS municipios_acima_743
    FROM `tech-challenge-fase2-507116`.gold.fato_indicador_alfabetizacao
    GROUP BY sigla_uf, ano
)
SELECT
    sigla_uf,
    ano,
    media_saeb_uf,
    taxa_media_uf,
    municipios_acima_743,
    LAG(media_saeb_uf) OVER (PARTITION BY sigla_uf ORDER BY ano) AS media_saeb_ano_anterior,
    ROUND(media_saeb_uf -
          LAG(media_saeb_uf) OVER (PARTITION BY sigla_uf ORDER BY ano), 2) AS variacao_saeb_yoy
FROM evolucao
ORDER BY sigla_uf, ano;

-- =====================================================================
-- Query 3: Top 20 Municípios Mais Próximos de Atingir a Meta (gap > -30)
-- =====================================================================
SELECT
    f.id_municipio,
    m.nome_municipio,
    f.sigla_uf,
    f.ano,
    ROUND(f.media_proficiencia_saeb, 2) AS media_saeb,
    ROUND(f.gap_proficiencia, 2) AS gap_para_corte,
    f.status_intervencao,
    ROUND(f.indicador_alfabetizacao, 2) AS indicador_alfabetizacao
FROM `tech-challenge-fase2-507116`.gold.fato_indicador_alfabetizacao f
LEFT JOIN `tech-challenge-fase2-507116`.gold.dim_municipio m USING (id_municipio)
WHERE f.ano = (SELECT MAX(ano) FROM `tech-challenge-fase2-507116`.gold.fato_indicador_alfabetizacao`)
  AND f.atingiu_corte_alfabetizado = FALSE
  AND f.gap_proficiencia > -30
ORDER BY f.gap_proficiencia DESC
LIMIT 20;

-- =====================================================================
-- Query 4: Comparativo Metas vs. Resultados por Região
-- =====================================================================
SELECT
    u.regiao,
    r.ano,
    COUNT(DISTINCT r.id_municipio)                             AS total_municipios,
    ROUND(AVG(r.meta_oficial), 2)                              AS meta_media_regiao,
    ROUND(AVG(r.indicador_real), 2)                            AS indicador_medio_regiao,
    ROUND(AVG(r.gap_percentual), 2)                            AS gap_medio_regiao,
    SUM(CASE WHEN r.status_cumprimento = 'ATINGIDA' THEN 1 ELSE 0 END)      AS municipios_atingiram,
    SUM(CASE WHEN r.status_cumprimento = 'PARCIAL' THEN 1 ELSE 0 END)       AS municipios_parciais,
    SUM(CASE WHEN r.status_cumprimento = 'NAO_ATINGIDA' THEN 1 ELSE 0 END)  AS municipios_nao_atingiram,
    ROUND(
        SUM(CASE WHEN r.status_cumprimento = 'ATINGIDA' THEN 1 ELSE 0 END) * 100.0
        / COUNT(DISTINCT r.id_municipio), 2
    ) AS perc_municipios_atingiram
FROM `tech-challenge-fase2-507116`.gold.fato_metas_vs_resultado r
LEFT JOIN `tech-challenge-fase2-507116`.gold.dim_uf u USING (sigla_uf)
GROUP BY u.regiao, r.ano
ORDER BY u.regiao, r.ano;
