{{ config(materialized='view') }}
-- =====================================================================
-- TASK 1 — stg_events (12 балів). Специфікація: ../../MODELS.md → «stg_events».
-- Прочитати партиційований Parquet і застосувати DQ-фільтри (типи, боти, порожні push).
-- Нижче — лише контракт колонок (заглушка повертає 0 рядків). Замініть тіло запиту.
-- =====================================================================

SELECT
    id::VARCHAR                       AS id,
    event_type::VARCHAR                     AS event_type,
    created_at::TIMESTAMPTZ           AS created_at,
    event_date::DATE                  AS event_date,
    actor_login::VARCHAR             AS actor_login,
    repo_name::VARCHAR               AS repo_name,
    payload_commit_count::BIGINT      AS payload_commit_count,
    payload_action::VARCHAR          AS payload_action,
    payload_ref::VARCHAR             AS payload_ref
FROM read_parquet(
        '{{ var("events_path") }}',
        hive_partitioning = true
     )
WHERE event_type IN (
        'PushEvent', 'IssuesEvent', 'PullRequestEvent',
        'WatchEvent', 'IssueCommentEvent'
      )
  AND actor_login NOT LIKE '%[bot]'
  AND NOT (event_type = 'PushEvent' AND payload_commit_count = 0)
