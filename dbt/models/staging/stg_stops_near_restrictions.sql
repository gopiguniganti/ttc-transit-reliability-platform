select
    stop_id,
    restriction_id,
    distance_m
from {{ source('serving', 'stops_near_restrictions') }}
