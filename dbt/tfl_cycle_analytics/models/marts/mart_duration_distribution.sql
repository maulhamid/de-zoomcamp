select
    duration_band,
    count(*) as trip_count,
    avg(duration_seconds) as avg_duration_seconds
from {{ ref('fct_trips') }}
group by 1

