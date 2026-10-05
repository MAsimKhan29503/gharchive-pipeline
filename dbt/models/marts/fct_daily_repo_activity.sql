select
    f.event_date,
    f.repo_id,
    count(*)                                                  as total_events,
    count(*) filter (where f.event_type = 'PushEvent')        as push_events,
    count(*) filter (where f.event_type = 'PullRequestEvent') as pull_request_events,
    count(*) filter (where f.event_type = 'IssuesEvent')      as issues_events,
    count(*) filter (where f.event_type = 'WatchEvent')       as stars,
    count(*) filter (where f.event_type = 'ForkEvent')        as forks,
    count(distinct f.actor_id)                                as unique_actors,
    count(distinct f.actor_id) filter (where not a.is_bot)    as unique_human_actors
from {{ ref('fct_events') }} f
join {{ ref('dim_actor') }} a using (actor_id)
group by f.event_date, f.repo_id