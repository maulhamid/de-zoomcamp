with latest_station_snapshot as (
    select * from {{ ref('stg_stations') }}
),
journey_station_lookup as (
    select
        start_station_id as station_id,
        max(start_station_name) as journey_station_name
    from {{ ref('stg_journeys') }}
    where start_station_id is not null
    group by 1

    union all

    select
        end_station_id as station_id,
        max(end_station_name) as journey_station_name
    from {{ ref('stg_journeys') }}
    where end_station_id is not null
    group by 1
),
journey_station_names as (
    select
        station_id,
        max(journey_station_name) as journey_station_name
    from journey_station_lookup
    group by 1
),
all_station_ids as (
    select station_id from latest_station_snapshot
    union
    select station_id from journey_station_names
)

select
    all_station_ids.station_id,
    latest_station_snapshot.station_key,
    coalesce(latest_station_snapshot.station_name, journey_station_names.journey_station_name) as station_name,
    latest_station_snapshot.place_type,
    latest_station_snapshot.lat,
    latest_station_snapshot.lon,
    latest_station_snapshot.terminal_name,
    latest_station_snapshot.nb_bikes,
    latest_station_snapshot.nb_empty_docks,
    latest_station_snapshot.nb_docks,
    latest_station_snapshot.install_date,
    latest_station_snapshot.locked,
    latest_station_snapshot.snapshot_ts
from all_station_ids
left join latest_station_snapshot
    on all_station_ids.station_id = latest_station_snapshot.station_id
left join journey_station_names
    on all_station_ids.station_id = journey_station_names.station_id
