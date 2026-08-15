select
    station_id,
    station_name,
    observed_at,
    temperature_c,
    condition,
    humidity_pct,
    wind_kmh,
    pressure_kpa
from {{ source('serving', 'fact_weather') }}
