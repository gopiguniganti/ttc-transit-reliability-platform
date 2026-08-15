-- dim_route is SCD2 (see docs/architecture.md Phase 3) -- only is_current
-- rows represent "this route, as it's defined today." Historical rows
-- exist for time-travel queries this project doesn't need yet, so they're
-- filtered out here rather than in every downstream model.
select
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    route_color,
    route_text_color
from {{ source('serving', 'dim_route') }}
where is_current
