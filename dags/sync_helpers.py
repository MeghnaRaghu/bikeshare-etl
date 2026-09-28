"""sync_helpers.py

Pure, framework-independent logic for the BigQuery -> Postgres Bikeshare ETL pipeline.
No Airflow imports here on purpose: this module is unit-tested directly
with pytest, without needing Airflow, BigQuery, or Postgres running.

Contains validation, reconciliation, referential-integrity, and
incremental-load helper functions used by the pipeline.
"""

from __future__ import annotations  # allows list[dict] syntax on Python 3.8



# ============================================================
# Bikeshare schema definitions
# ============================================================

EXPECTED_STATION_SCHEMA = {"station_id": int, "name": str}
EXPECTED_TRIP_SCHEMA = {
    "trip_id": int,
    "start_station_id": int,
    "end_station_id": int,
}


# ============================================================
# Shared / generic helpers
# ============================================================

def check_reconciliation(source_count: int, destination_count: int) -> None:
    """
    Confirms the destination has at least as many rows as the source claimed.
    Raises ValueError if the destination is short (data loss / partial load).
    Destination can legitimately be >= source count on a re-run (upserts don't
    remove rows from prior runs of a superset source), so we don't fail on ">".
    """
    if destination_count < source_count:
        raise ValueError(
            f"Reconciliation failed: destination has {destination_count} rows, "
            f"expected at least {source_count}."
        )


# ============================================================
# ============================================================


def validate_stations(rows: list[dict]) -> dict:
    """
    Schema + data-quality checks for the stations dimension table.
    station_id and name are required (used as the primary key / join target).
    """
    if not rows:
        raise ValueError("Station validation failed: no station rows extracted.")

    missing_columns = [col for col in EXPECTED_STATION_SCHEMA if col not in rows[0]]
    if missing_columns:
        raise ValueError(f"Station validation failed: missing columns {missing_columns}")

    null_ids = sum(1 for r in rows if r.get("station_id") is None)
    null_names = sum(1 for r in rows if not r.get("name"))

    if null_ids > 0:
        raise ValueError(f"Station validation failed: {null_ids} rows with null station_id.")
    if null_names > 0:
        raise ValueError(f"Station validation failed: {null_names} rows with null name.")

    duplicate_station_ids = len(rows) - len({r["station_id"] for r in rows})

    return {"total_rows": len(rows), "duplicate_station_ids": duplicate_station_ids}


def validate_trips_batch(rows: list[dict]) -> dict:
    """
    Schema + data-quality checks for one batch of trip (fact) rows.
    Runs per-batch during extraction, not on the whole dataset at once -
    this is what lets validation scale to a huge source without loading
    everything into memory simultaneously.
    """
    if not rows:
        raise ValueError("Trip batch validation failed: empty batch.")

    missing_columns = [col for col in EXPECTED_TRIP_SCHEMA if col not in rows[0]]
    if missing_columns:
        raise ValueError(f"Trip batch validation failed: missing columns {missing_columns}")

    null_trip_ids = sum(1 for r in rows if r.get("trip_id") is None)
    if null_trip_ids > 0:
        raise ValueError(f"Trip batch validation failed: {null_trip_ids} rows with null trip_id.")

    return {"total_rows": len(rows), "null_trip_ids": null_trip_ids}


def check_referential_integrity(trip_rows: list[dict], station_ids: set) -> int:
    """
    Counts trips whose start_station_id or end_station_id doesn't exist in
    the stations dimension table. This is a join-based data-quality check
    performed BEFORE the data ever reaches Postgres - catching orphaned
    foreign keys early rather than discovering them as broken joins later.

    Real-world source data can legitimately contain some orphan references
    (e.g. a station that was later decommissioned/renamed), so this returns
    a count for visibility rather than raising - the caller decides whether
    the count is acceptable.

    Type normalization: the source has genuinely mixed types across the two
    FK columns (start_station_id is INT64, end_station_id is STRING, per
    BigQuery's own schema) - comparing either directly against a set of
    station_id ints would silently mismatch every STRING-typed end_station_id
    value (e.g. "4051" != 4051 in a Python set), producing false-positive
    orphans on every row. Everything is compared as a string to avoid this.
    """
    normalized_station_ids = {str(sid) for sid in station_ids}

    orphans = 0
    for r in trip_rows:
        start_id = r.get("start_station_id")
        end_id = r.get("end_station_id")
        start_missing = start_id is not None and str(start_id) not in normalized_station_ids
        end_missing = end_id is not None and str(end_id) not in normalized_station_ids
        if start_missing or end_missing:
            orphans += 1
    return orphans


# ------------------------------------------------------------------
# Watermark / incremental-load helpers
# ------------------------------------------------------------------

def max_timestamp(timestamps: list) -> str | None:
    """
    Given a list of ISO-format datetime strings, datetime objects, or None
    entries, returns the maximum as an ISO-format string, ignoring None
    values. Returns None if the list is empty or every entry is None.

    Pure - used to compute a new watermark candidate from one batch of
    extracted rows, with no DB/Airflow dependency.
    """
    from datetime import datetime

    parsed = []
    for ts in timestamps:
        if ts is None:
            continue
        if isinstance(ts, datetime):
            parsed.append(ts)
        else:
            parsed.append(datetime.fromisoformat(str(ts)))

    if not parsed:
        return None
    return max(parsed).isoformat()


def build_watermark_filter(watermark: str | None) -> str:
    """
    Returns the additional SQL WHERE-clause fragment for an incremental
    extract. Empty string on a full load (no watermark recorded yet, i.e.
    this pipeline has never run successfully before).
    """
    if not watermark:
        return ""
    return "AND start_time > @watermark"