{% set partition_config = {'field': 'started_date', 'data_type': 'date'} if target.type == 'bigquery' else none %}
{% set cluster_config = ['start_station_id', 'end_station_id'] if target.type == 'bigquery' else none %}

{{ config(
    partition_by=partition_config,
    cluster_by=cluster_config
) }}

with journeys as (
    select * from {{ ref('stg_journeys') }}
),
stations as (
    select * from {{ ref('dim_station') }}
)

select
    journeys.trip_id,
    journeys.rental_id,
    journeys.bike_id,
    journeys.started_at,
    journeys.ended_at,
    journeys.started_date,
    journeys.ended_date,
    journeys.start_station_id,
    journeys.start_station_name,
    coalesce(start_station.station_name, journeys.start_station_name) as start_station_name_canonical,
    journeys.end_station_id,
    journeys.end_station_name,
    coalesce(end_station.station_name, journeys.end_station_name) as end_station_name_canonical,
    journeys.duration_seconds,
    journeys.duration_band,
    journeys.hour_of_day,
    journeys.day_of_week,
    journeys.is_weekend
from journeys
left join stations as start_station
    on journeys.start_station_id = start_station.station_id
left join stations as end_station
    on journeys.end_station_id = end_station.station_id

