select event_date, repo_id, count(*)
from {{ ref('fct_daily_repo_activity') }}
group by 1, 2
having count(*) > 1