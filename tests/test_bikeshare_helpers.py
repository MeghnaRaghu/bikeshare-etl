"""
test_sync_helpers.py

Unit tests for sync_helpers.py - pure logic only, no Airflow/BigQuery/Postgres
needed to run these. Run with:
    pytest test_sync_helpers.py -v --cov=sync_helpers --cov-report=term-missing
"""

import pytest
from sync_helpers import (
    check_reconciliation,
    validate_stations,
    validate_trips_batch,
    check_referential_integrity,
    max_timestamp,
    build_watermark_filter,
)


# ---- check_reconciliation (shared) ----

def test_check_reconciliation_passes_when_equal():
    check_reconciliation(source_count=100, destination_count=100)  # no exception


def test_check_reconciliation_passes_when_destination_larger():
    # destination can be >= source on a re-run against a growing/superset source
    check_reconciliation(source_count=100, destination_count=150)  # no exception


def test_check_reconciliation_fails_when_destination_short():
    with pytest.raises(ValueError, match="Reconciliation failed"):
        check_reconciliation(source_count=100, destination_count=90)


# ---- validate_stations ----

def test_validate_stations_happy_path():
    rows = [
        {"station_id": 1, "name": "Station A"},
        {"station_id": 2, "name": "Station B"},
    ]
    summary = validate_stations(rows)
    assert summary["total_rows"] == 2
    assert summary["duplicate_station_ids"] == 0


def test_validate_stations_empty_raises():
    with pytest.raises(ValueError, match="no station rows"):
        validate_stations([])


def test_validate_stations_missing_column_raises():
    rows = [{"station_id": 1}]  # missing name
    with pytest.raises(ValueError, match="missing columns"):
        validate_stations(rows)


def test_validate_stations_null_id_raises():
    rows = [{"station_id": None, "name": "Station A"}]
    with pytest.raises(ValueError, match="null station_id"):
        validate_stations(rows)


def test_validate_stations_null_name_raises():
    rows = [{"station_id": 1, "name": ""}]
    with pytest.raises(ValueError, match="null name"):
        validate_stations(rows)


def test_validate_stations_detects_duplicates():
    rows = [
        {"station_id": 1, "name": "Station A"},
        {"station_id": 1, "name": "Station A (dup)"},
    ]
    summary = validate_stations(rows)
    assert summary["duplicate_station_ids"] == 1


# ---- validate_trips_batch ----

def test_validate_trips_batch_happy_path():
    rows = [
        {"trip_id": 101, "start_station_id": 1, "end_station_id": 2},
        {"trip_id": 102, "start_station_id": 2, "end_station_id": 1},
    ]
    summary = validate_trips_batch(rows)
    assert summary["total_rows"] == 2
    assert summary["null_trip_ids"] == 0


def test_validate_trips_batch_empty_raises():
    with pytest.raises(ValueError, match="empty batch"):
        validate_trips_batch([])


def test_validate_trips_batch_missing_column_raises():
    rows = [{"trip_id": 101, "start_station_id": 1}]  # missing end_station_id
    with pytest.raises(ValueError, match="missing columns"):
        validate_trips_batch(rows)


def test_validate_trips_batch_null_trip_id_raises():
    rows = [{"trip_id": None, "start_station_id": 1, "end_station_id": 2}]
    with pytest.raises(ValueError, match="null trip_id"):
        validate_trips_batch(rows)


# ---- check_referential_integrity ----

def test_check_referential_integrity_no_orphans():
    trip_rows = [
        {"trip_id": 1, "start_station_id": 10, "end_station_id": 20},
        {"trip_id": 2, "start_station_id": 20, "end_station_id": 10},
    ]
    station_ids = {10, 20}
    assert check_referential_integrity(trip_rows, station_ids) == 0


def test_check_referential_integrity_detects_orphan_start_station():
    trip_rows = [{"trip_id": 1, "start_station_id": 999, "end_station_id": 20}]
    station_ids = {10, 20}
    assert check_referential_integrity(trip_rows, station_ids) == 1


def test_check_referential_integrity_detects_orphan_end_station():
    trip_rows = [{"trip_id": 1, "start_station_id": 10, "end_station_id": 999}]
    station_ids = {10, 20}
    assert check_referential_integrity(trip_rows, station_ids) == 1


def test_check_referential_integrity_counts_each_bad_row_once():
    # a row with BOTH station ids missing still counts once, not twice
    trip_rows = [{"trip_id": 1, "start_station_id": 998, "end_station_id": 999}]
    station_ids = {10, 20}
    assert check_referential_integrity(trip_rows, station_ids) == 1


def test_check_referential_integrity_allows_null_station_ids():
    # a null station id is a schema/quality issue (caught elsewhere),
    # not a referential-integrity orphan - shouldn't be double-counted here
    trip_rows = [{"trip_id": 1, "start_station_id": None, "end_station_id": 20}]
    station_ids = {10, 20}
    assert check_referential_integrity(trip_rows, station_ids) == 0


def test_check_referential_integrity_handles_mixed_int_and_string_types():
    # Regression test: source has start_station_id as INT64 but
    # end_station_id as STRING (confirmed via BigQuery INFORMATION_SCHEMA).
    # A naive `value not in station_ids` comparison would treat "10" and 10
    # as different, falsely flagging every string-typed match as an orphan.
    trip_rows = [
        {"trip_id": 1, "start_station_id": 10, "end_station_id": "20"},   # int start, string end - both valid
        {"trip_id": 2, "start_station_id": "10", "end_station_id": 20},  # string start, int end - both valid
    ]
    station_ids = {10, 20}  # station_ids always arrive as int (dim table's actual type)
    assert check_referential_integrity(trip_rows, station_ids) == 0


def test_check_referential_integrity_still_detects_genuine_orphan_with_mixed_types():
    trip_rows = [{"trip_id": 1, "start_station_id": 10, "end_station_id": "Event"}]
    station_ids = {10, 20}
    assert check_referential_integrity(trip_rows, station_ids) == 1


# ---- max_timestamp ----

def test_max_timestamp_returns_latest_of_strings():
    timestamps = ["2020-01-01T00:00:00", "2021-05-05T10:00:00", "2019-12-31T23:59:59"]
    assert max_timestamp(timestamps) == "2021-05-05T10:00:00"


def test_max_timestamp_ignores_none_entries():
    timestamps = [None, "2020-06-15T12:00:00", None]
    assert max_timestamp(timestamps) == "2020-06-15T12:00:00"


def test_max_timestamp_empty_list_returns_none():
    assert max_timestamp([]) is None


def test_max_timestamp_all_none_returns_none():
    assert max_timestamp([None, None, None]) is None


def test_max_timestamp_accepts_datetime_objects():
    from datetime import datetime

    timestamps = [datetime(2020, 1, 1), datetime(2022, 3, 3), datetime(2021, 6, 6)]
    assert max_timestamp(timestamps) == datetime(2022, 3, 3).isoformat()


def test_max_timestamp_single_value():
    assert max_timestamp(["2023-07-04T08:00:00"]) == "2023-07-04T08:00:00"


# ---- build_watermark_filter ----

def test_build_watermark_filter_none_returns_empty_string():
    assert build_watermark_filter(None) == ""


def test_build_watermark_filter_empty_string_returns_empty_string():
    assert build_watermark_filter("") == ""


def test_build_watermark_filter_with_value_returns_clause():
    result = build_watermark_filter("2023-01-01T00:00:00")
    assert result == "AND start_time > @watermark"