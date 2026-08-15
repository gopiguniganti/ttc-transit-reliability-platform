select
    restriction_id,
    road,
    road_class,
    latitude,
    longitude,
    planned,
    expired,
    start_time,
    end_time,
    description
from {{ source('serving', 'road_restrictions') }}
where not expired
