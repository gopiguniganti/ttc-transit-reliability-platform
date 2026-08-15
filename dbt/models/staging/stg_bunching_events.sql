-- feed_timestamp is a raw unix epoch (seconds) from the GTFS-RT poll snapshot --
-- converting it here, once, so every downstream model works with a real
-- timestamp instead of every consumer re-deriving it.
select
    route_id,
    route_short_name,
    to_timestamp(feed_timestamp) as event_time,
    vehicle_id_a,
    vehicle_id_b,
    stop_sequence_gap,
    detected_at
from {{ source('serving', 'bunching_events') }}
