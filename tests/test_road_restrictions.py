"""haversine_m and find_nearby_stops are pure functions -- no DB, no
network -- so they're what's actually tested here."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "context_data"))

from load_road_restrictions import haversine_m, find_nearby_stops


def test_haversine_same_point_is_zero():
    assert haversine_m(43.65, -79.38, 43.65, -79.38) == 0


def test_haversine_known_distance_is_approximately_right():
    # 1 degree of latitude is ~111.19km, everywhere on Earth -- a precise
    # known distance to check against, unlike guessing a landmark distance.
    d = haversine_m(43.65, -79.38, 44.65, -79.38)
    assert 111_000 < d < 111_500


def test_find_nearby_stops_finds_close_pair():
    stops = [{"stop_id": "A", "lat": 43.6500, "lon": -79.3800}]
    restrictions = [{"restriction_id": "R1", "lat": 43.6501, "lon": -79.3801}]
    result = find_nearby_stops(stops, restrictions, radius_m=150)
    assert len(result) == 1
    assert result[0]["stop_id"] == "A"
    assert result[0]["restriction_id"] == "R1"


def test_find_nearby_stops_excludes_far_pair():
    stops = [{"stop_id": "A", "lat": 43.6500, "lon": -79.3800}]
    restrictions = [{"restriction_id": "R1", "lat": 43.7000, "lon": -79.4200}]  # several km away
    result = find_nearby_stops(stops, restrictions, radius_m=150)
    assert result == []
