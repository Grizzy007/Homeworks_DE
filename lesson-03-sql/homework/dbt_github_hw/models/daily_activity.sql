-- =====================================================================
-- TASK 3 — daily_activity (12 балів). Специфікація: ../../MODELS.md → «daily_activity».
-- Кількість подій по днях + накопичувальний підсумок: SUM(...) OVER (ORDER BY ...).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
WITH cte AS(
    SELECT
        event_date,
        COUNT(*)::BIGINT AS events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date
     )
SELECT *,  
    SUM(events) OVER (ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::BIGINT  AS running_events
FROM cte
