select distinct on (repo_id)
    repo_id,
    repo_name,
    created_at as last_seen_at
from {{ ref('stg_events') }}
where repo_id is not null
order by repo_id, created_at desc