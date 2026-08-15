-- Singular test: a vehicle can never be "bunched" with itself. This exact
-- bug shipped once already -- a Kafka crash-loop during a backlog replay
-- left duplicate (vehicle_id, feed_timestamp) rows in bronze, which made
-- the bunching window function match a vehicle against its own duplicate.
-- 44% of events were self-matches before the fix (dropDuplicates in
-- notebooks/02_silver_bunching.py). This test fails the dbt run if it
-- ever comes back, instead of silently shipping bad data to Grafana again.
select *
from {{ ref('mart_bunching_events') }}
where vehicle_id_a = vehicle_id_b
