-- gold.dim_driver — ЕТАП 2. Grain: водій. SPEC.md, розділ 4.4.
--   * incremental, unique_key='driver_key', delete+insert; за подіями ride_accepted у ref('events')
--   * «останній» рейтинг — за occurred_at; перераховуйте водія з УСІЄЇ його історії, не з батча
--   * плюс член driver_key = 'unknown' (поїздки, скасовані до прийняття)

{{ config(
    materialized='incremental',
    unique_key='driver_key',
    incremental_strategy='delete+insert',
    tags=['gold']
) }}

with accepted as (
    select
        payload->'driver'->>'id'                      as driver_key,
        payload->'driver'->'vehicle'->>'type'         as vehicle_type,
        payload->'driver'->'vehicle'->>'medallion'    as medallion,
        (payload->'driver'->>'rating')::numeric       as rating,
        occurred_at,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
      and payload->'driver'->>'id' is not null
),
touched as (
    select distinct driver_key
    from accepted
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
),
hist as (
    select a.*
    from accepted a
    join touched t on a.driver_key = t.driver_key
),
ranked as (
    select *,
        row_number() over (
            partition by driver_key
            order by occurred_at desc, _ingested_at desc
        ) as rn
    from hist
),
agg as (
    select
        driver_key,
        max(vehicle_type) filter (where rn = 1) as vehicle_type,
        max(medallion)    filter (where rn = 1) as medallion,
        max(rating)       filter (where rn = 1) as latest_rating,
        min(occurred_at)  as first_seen_at,
        max(occurred_at)  as last_seen_at,
        max(_ingested_at) as _ingested_at
    from ranked
    group by driver_key
)
select
    driver_key, vehicle_type, medallion, latest_rating,
    first_seen_at, last_seen_at, _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from agg

{% if not is_incremental() %}
union all
select 'unknown', null, null, null, null, null, null,
       '{{ run_started_at }}'::timestamptz
{% endif %}
