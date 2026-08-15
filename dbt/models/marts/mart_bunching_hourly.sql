-- Grafana-friendly rollup: one row per (route, hour), for a time-series
-- panel showing which routes bunch the most and when -- the individual-event
-- grain of mart_bunching_events is too dense to plot directly over days of
-- data.
select
    route_id,
    route_short_name,
    route_long_name,
    date_trunc('hour', event_time) as event_hour,
    count(*) as bunching_event_count,
    avg(stop_sequence_gap) as avg_stop_sequence_gap
from {{ ref('mart_bunching_events') }}
group by 1, 2, 3, 4
