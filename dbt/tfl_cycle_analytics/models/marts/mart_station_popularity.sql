select
    start_station_id as station_id,
    start_station_name_canonical as station_name,
    count(*) as departure_trip_count,
    avg(duration_seconds) as avg_duration_seconds
from {{ ref('fct_trips') }}
where start_station_id is not null
group by 1, 2

