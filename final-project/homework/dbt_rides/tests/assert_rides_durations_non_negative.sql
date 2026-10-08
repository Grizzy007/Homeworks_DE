-- Правило: тривалості не можуть бути відʼємні (інакше переплутано початок/кінець
-- або часові пояси в арифметиці). Тест падає, якщо знайде такий рядок.
select ride_id, wait_seconds, trip_seconds
from {{ ref('rides') }}
where wait_seconds < 0
   or trip_seconds < 0