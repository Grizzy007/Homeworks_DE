-- =====================================================================
-- TASK 6 — mart_category_daily (20 балів). Специфікація: ../../MODELS.md → «mart_category_daily».
-- Широка вітрина: multi-join stg_events + event_categories + calendar, агрегація по (день × категорія).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    e.event_date,
    c.is_weekend,
    ec.category,
    COUNT(*)::BIGINT                        AS events,
    COUNT(DISTINCT e.repo_name)::BIGINT     AS distinct_repos,
    COUNT(DISTINCT e.actor_login)::BIGINT   AS distinct_actors
FROM {{ ref('stg_events') }} AS e
INNER JOIN {{ ref('calendar') }} AS c
       ON e.event_date = c.day
INNER JOIN {{ ref('event_categories') }} AS ec
       ON e.event_type = ec.event_type
GROUP BY e.event_date, c.is_weekend, ec.category