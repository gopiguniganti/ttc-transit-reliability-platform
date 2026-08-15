select
    stop_id,
    stop_code,
    stop_name,
    stop_lat,
    stop_lon,
    wheelchair_boarding
from {{ source('serving', 'dim_stop') }}
where is_current
