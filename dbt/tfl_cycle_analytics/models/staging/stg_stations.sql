select
    cast(station_id as bigint) as station_id,
    cast(station_key as {{ dbt.type_string() }}) as station_key,
    cast(station_name as {{ dbt.type_string() }}) as station_name,
    cast(place_type as {{ dbt.type_string() }}) as place_type,
    cast(lat as double) as lat,
    cast(lon as double) as lon,
    cast(terminal_name as {{ dbt.type_string() }}) as terminal_name,
    cast(nb_bikes as bigint) as nb_bikes,
    cast(nb_empty_docks as bigint) as nb_empty_docks,
    cast(nb_docks as bigint) as nb_docks,
    cast(install_date as {{ dbt.type_string() }}) as install_date,
    cast(locked as {{ dbt.type_string() }}) as locked,
    cast(snapshot_ts as timestamp) as snapshot_ts
from {{ source('raw', 'stations_latest') }}
where station_id is not null
