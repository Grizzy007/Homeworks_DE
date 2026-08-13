-- =====================================================================
-- TASK 4 — daily_activity_change (12 балів). Специфікація: ../../MODELS.md → «daily_activity_change».
-- Зміна кількості подій день-до-дня: LAG(...) OVER (ORDER BY ...).
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
    LAG(events) OVER (ORDER BY event_date)::BIGINT  AS prev_day_events,
    events - prev_day_events AS delta_events
FROM cte

