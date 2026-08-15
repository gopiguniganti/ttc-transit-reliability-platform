-- The "why was this bus bunched" mart: one row per detected bunching pair,
-- enriched with the route's real name and whatever weather was closest in
-- time to the event.
--
-- Road restrictions are NOT joined in here yet -- bunching_events only
-- carries route_id + stop_sequence, not a stop_id or lat/lon, so there's no
-- location to join stops_near_restrictions against. Fixing that means
-- capturing the stop location at detection time in
-- notebooks/02_silver_bunching.py, not something dbt can add after the
-- fact. Documented as a real gap, not silently skipped -- see
-- docs/architecture.md.
--
-- Indexes: dbt-postgres creates these after the table is built (not just
-- documentation) -- without them, Grafana's "recent events" panel was
-- doing a full parallel sequential scan across all 4.5M+ rows on every
-- dashboard load (~2s, confirmed via EXPLAIN ANALYZE) just to sort by
-- event_time and take the top 200.
{{ config(indexes=[
    {'columns': ['event_time'], 'type': 'btree'},
    {'columns': ['route_id'], 'type': 'btree'},
]) }}

with bunching as (
    select * from {{ ref('stg_bunching_events') }}
),

routes as (
    select * from {{ ref('stg_dim_route_current') }}
)

select
    b.route_id,
    coalesce(r.route_short_name, b.route_short_name) as route_short_name,
    r.route_long_name,
    r.route_color,
    b.event_time,
    b.vehicle_id_a,
    b.vehicle_id_b,
    b.stop_sequence_gap,
    b.detected_at,
    w.temperature_c,
    w.condition as weather_condition,
    w.wind_kmh
from bunching b
left join routes r on r.route_id = b.route_id
-- LATERAL: re-runs this subquery per bunching row with that row's
-- event_time in scope, picking the single closest weather observation in
-- time -- an ordinary JOIN has no way to reference the left side's columns
-- like this.
left join lateral (
    select temperature_c, condition, wind_kmh
    from {{ ref('stg_fact_weather') }} w
    order by abs(extract(epoch from (w.observed_at - b.event_time)))
    limit 1
) w on true
