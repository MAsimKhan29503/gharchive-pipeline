select distinct on (actor_id)
    actor_id,
    actor_login,
    actor_login like '%[bot]' as is_bot,
    created_at as last_seen_at
from {{ ref('stg_events') }}
where actor_id is not null
order by actor_id, created_at desc