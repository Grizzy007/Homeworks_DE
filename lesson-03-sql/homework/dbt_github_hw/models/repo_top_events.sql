-- =====================================================================
-- TASK 2 — repo_top_events (12 балів). Специфікація: ../../MODELS.md → «repo_top_events».
-- TOP-5 репозиторіїв за кількістю подій у кожному event_type: ROW_NUMBER() + QUALIFY.
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
WITH cte AS(
    SELECT
        event_type::VARCHAR AS event_type,
        repo_name::VARCHAR AS repo_name,
        COUNT(*)::BIGINT  AS event_count
    FROM {{ ref('stg_events') }}
    GROUP BY event_type, repo_name
     )
SELECT *,  
    ROW_NUMBER() OVER(PARTITION BY event_type ORDER BY event_count DESC, repo_name)::BIGINT  AS type_rank
FROM cte
QUALIFY type_rank <=5

