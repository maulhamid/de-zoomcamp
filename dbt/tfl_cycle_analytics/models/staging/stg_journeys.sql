select
    cast(trip_id as {{ dbt.type_string() }}) as trip_id,
    cast(rental_id as {{ dbt.type_string() }}) as rental_id,
    cast(bike_id as {{ dbt.type_string() }}) as bike_id,
    cast(started_at as timestamp) as started_at,
    cast(ended_at as timestamp) as ended_at,
    cast(started_date as date) as started_date,
    cast(ended_date as date) as ended_date,
    cast(start_station_id as bigint) as start_station_id,
    cast(start_station_name as {{ dbt.type_string() }}) as start_station_name,
    cast(end_station_id as bigint) as end_station_id,
    cast(end_station_name as {{ dbt.type_string() }}) as end_station_name,
    cast(duration_seconds as {{ dbt.type_int() }}) as duration_seconds,
    cast(duration_band as {{ dbt.type_string() }}) as duration_band,
    cast(hour_of_day as {{ dbt.type_int() }}) as hour_of_day,
    cast(day_of_week as {{ dbt.type_string() }}) as day_of_week,
    cast(is_weekend as boolean) as is_weekend
from {{ source('raw', 'journeys') }}
where started_at is not null
  and ended_at is not null
