select
    event_id,
    event_type,
    created_at,
    (payload -> 'actor' ->> 'id')::bigint   as actor_id,
    payload -> 'actor' ->> 'login'          as actor_login,
    (payload -> 'repo' ->> 'id')::bigint    as repo_id,
    payload -> 'repo' ->> 'name'            as repo_name,
    payload -> 'payload' ->> 'action'       as action,
    source_file,
    loaded_at
from {{ source('raw', 'gh_events') }}