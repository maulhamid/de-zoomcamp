select
    started_date,
    count(*) as trip_count,
    avg(duration_seconds) as avg_duration_seconds,
    sum(case when is_weekend then 1 else 0 end) as weekend_trip_count
from {{ ref('fct_trips') }}
group by 1

