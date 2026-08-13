"""diff_scd2 is the only part of the GTFS static loader worth unit testing
without a real Postgres -- it's a pure function, the DB calls around it
are just SELECT/UPDATE/INSERT with no logic of their own."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "gtfs_static"))

from load_dimensions import diff_scd2

TRACKED = ["route_short_name", "route_long_name"]


def test_new_key_is_inserted_nothing_expired():
    current = {}
    new = [{"route_id": "504", "route_short_name": "504", "route_long_name": "King"}]
    to_insert, to_expire = diff_scd2(current, new, "route_id", TRACKED)
    assert to_insert == new
    assert to_expire == []


def test_unchanged_row_is_left_alone():
    row = {"route_id": "504", "route_short_name": "504", "route_long_name": "King"}
    current = {"504": row}
    to_insert, to_expire = diff_scd2(current, [row], "route_id", TRACKED)
    assert to_insert == []
    assert to_expire == []


def test_changed_attribute_expires_old_and_inserts_new():
    current = {"504": {"route_id": "504", "route_short_name": "504", "route_long_name": "King"}}
    new = [{"route_id": "504", "route_short_name": "504", "route_long_name": "King Streetcar"}]
    to_insert, to_expire = diff_scd2(current, new, "route_id", TRACKED)
    assert to_insert == new
    assert to_expire == ["504"]


def test_falsy_zero_value_is_not_treated_as_a_change():
    # regression: route_type 0 (streetcar) from the DB (a real int) vs "0"
    # from the CSV (a string) must compare equal, not look like a change
    # just because 0 is falsy in Python.
    current = {"504": {"route_id": "504", "route_short_name": "504", "route_type": 0}}
    new = [{"route_id": "504", "route_short_name": "504", "route_type": "0"}]
    to_insert, to_expire = diff_scd2(current, new, "route_id", ["route_short_name", "route_type"])
    assert to_insert == []
    assert to_expire == []


def test_key_missing_from_new_source_is_expired():
    current = {"999": {"route_id": "999", "route_short_name": "999", "route_long_name": "Discontinued"}}
    to_insert, to_expire = diff_scd2(current, [], "route_id", TRACKED)
    assert to_insert == []
    assert to_expire == ["999"]
