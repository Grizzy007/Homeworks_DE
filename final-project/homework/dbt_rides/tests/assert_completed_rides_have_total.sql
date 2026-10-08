-- Правило: завершена поїздка мусить мати підсумкову суму (fare.total прийшов).
-- Ловить completed без total_amount — ознаку, що згортку поїздки зібрано неправильно.
select ride_id, status, total_amount
from {{ ref('fact_ride') }}
where status = 'completed'
  and total_amount is null