{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

select
    event_id,
    event_type,
    created_at,
    created_at::date as event_date,
    repo_id,
    actor_id,
    action,
    loaded_at
from {{ ref('stg_events') }}
where repo_id is not null
{% if is_incremental() %}
  and loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}