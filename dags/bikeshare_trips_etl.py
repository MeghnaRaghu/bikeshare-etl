"""
bikeshare_trips_etl.py

BigQuery -> PostgreSQL ETL pipeline for Austin Bikeshare data.

The pipeline:
  - Extracts station and trip data from BigQuery in paginated batches.
  - Loads trip data into PostgreSQL using COPY through a staging table.
  - Uses fact and dimension tables for the core data model.
  - Builds derived insight tables/views using SQL joins and aggregations.
  - Creates indexes after bulk loading for efficient query performance.
  - Supports configurable batch size and staging directory through
    Airflow Variables.
  - Maintains a pipeline watermark for incremental processing.
"""

import csv
import io
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from sync_helpers import (
    validate_stations,
    validate_trips_batch,
    check_referential_integrity,
    check_reconciliation,
    max_timestamp,
    build_watermark_filter,
)

WATERMARK_PIPELINE_NAME = "bikeshare_trips"

STATIONS_QUERY = """
    SELECT station_id, name, status, number_of_docks, location
    FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations`
    WHERE station_id IS NOT NULL
"""


def _staging_dir() -> str:
    # Airflow Variable, not a hardcoded path - lets the same DAG code point
    # at different staging locations per environment without editing this file.
    return Variable.get("dag_staging_dir", default_var="/tmp/pip_dag_staging")


def _batch_size() -> int:
    return int(Variable.get("dag_batch_size", default_var="50000"))


def _stations_staging_path() -> str:
    return os.path.join(_staging_dir(), "stations.json")


# ------------------------------------------------------------------
# Watermark (incremental load state)
# ------------------------------------------------------------------

def get_watermark(**context):
    """
    Reads the last-synced timestamp for this pipeline from a small
    watermark table. Returns None on the very first run (table/row
    doesn't exist yet) - extract_trips_from_bigquery treats None as
    "do a full load".
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_destination")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_watermark (
            pipeline_name TEXT PRIMARY KEY,
            last_synced_start_time TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now()
        );
        """
    )
    conn.commit()

    cur.execute(
        "SELECT last_synced_start_time FROM pipeline_watermark WHERE pipeline_name = %s;",
        (WATERMARK_PIPELINE_NAME,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    watermark = row[0].isoformat() if row and row[0] else None
    print(f"Current watermark for '{WATERMARK_PIPELINE_NAME}': {watermark or '(none - full load)'}")
    context["ti"].xcom_push(key="watermark", value=watermark)


# ------------------------------------------------------------------
# Extraction (parallel branches: stations is small, trips is batched)
# ------------------------------------------------------------------

def extract_stations_from_bigquery(**context):
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook(gcp_conn_id="bigquery_default", use_legacy_sql=False)
    client = hook.get_client()

    rows = [dict(row) for row in client.query(STATIONS_QUERY).result()]

    os.makedirs(_staging_dir(), exist_ok=True)
    with open(_stations_staging_path(), "w") as f:
        json.dump(rows, f)

    print(f"Extracted {len(rows)} station rows.")
    context["ti"].xcom_push(key="extracted_station_count", value=len(rows))


def extract_trips_from_bigquery(**context):
    """
    Extracts the fact table in PAGES, writing each page to its own batch
    file. This is the batching pattern: at no point is the full result
    set held in memory - only one page (default 50,000 rows) at a time.

    Incremental: pulls only rows newer than the current watermark (or
    everything, on the first run when no watermark exists yet). Tracks
    the max start_time seen across every batch as the candidate new
    watermark - only committed to the watermark table later, by
    update_watermark, and only after reconcile has passed.
    """
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from google.cloud import bigquery as bq

    ti = context["ti"]
    watermark = ti.xcom_pull(task_ids="get_watermark", key="watermark")

    hook = BigQueryHook(gcp_conn_id="bigquery_default", use_legacy_sql=False)
    client = hook.get_client()

    watermark_clause = build_watermark_filter(watermark)
    query = f"""
        SELECT trip_id, subscriber_type, bike_type, start_time,
               start_station_id, end_station_id, duration_minutes
        FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
        WHERE trip_id IS NOT NULL {watermark_clause}
    """

    job_config = None
    if watermark:
        job_config = bq.QueryJobConfig(
            query_parameters=[bq.ScalarQueryParameter("watermark", "TIMESTAMP", watermark)]
        )

    batch_size = _batch_size()
    staging_dir = _staging_dir()
    os.makedirs(staging_dir, exist_ok=True)

    query_job = client.query(query, job_config=job_config) if job_config else client.query(query)
    row_iterator = query_job.result(page_size=batch_size)

    batch_files = []
    total = 0
    batch_max_timestamps = []
    for page_num, page in enumerate(row_iterator.pages, start=1):
        batch_rows = [dict(row) for row in page]
        if not batch_rows:
            continue
        batch_path = os.path.join(staging_dir, f"trips_batch_{page_num:04d}.json")
        with open(batch_path, "w") as f:
            json.dump(batch_rows, f, default=str)  # default=str handles datetime
        batch_files.append(batch_path)
        total += len(batch_rows)
        batch_max_timestamps.append(max_timestamp([r.get("start_time") for r in batch_rows]))
        print(f"Batch {page_num}: {len(batch_rows)} rows -> {batch_path}")

    new_watermark = max_timestamp(batch_max_timestamps)

    mode = f"incremental (> {watermark})" if watermark else "full load (no prior watermark)"
    print(f"Extracted {total} trip rows across {len(batch_files)} batches ({mode}).")
    ti.xcom_push(key="trip_batch_files", value=batch_files)
    ti.xcom_push(key="extracted_trip_count", value=total)
    ti.xcom_push(key="new_watermark", value=new_watermark)


# ------------------------------------------------------------------
# Validation (schema + data quality + referential integrity, per batch)
# ------------------------------------------------------------------

def validate_extract(**context):
    ti = context["ti"]

    with open(_stations_staging_path()) as f:
        station_rows = json.load(f)

    try:
        station_summary = validate_stations(station_rows)
    except ValueError as e:
        raise AirflowException(str(e))
    print(f"Station validation: {station_summary}")

    station_ids = {r["station_id"] for r in station_rows}

    batch_files = ti.xcom_pull(task_ids="extract_trips_from_bigquery", key="trip_batch_files")

    total_trip_rows = 0
    total_orphans = 0
    for batch_path in batch_files:
        with open(batch_path) as f:
            trip_rows = json.load(f)
        try:
            batch_summary = validate_trips_batch(trip_rows)
        except ValueError as e:
            raise AirflowException(f"{os.path.basename(batch_path)}: {e}")

        orphans = check_referential_integrity(trip_rows, station_ids)
        total_trip_rows += batch_summary["total_rows"]
        total_orphans += orphans
        print(f"{os.path.basename(batch_path)}: {batch_summary}, orphan_station_refs={orphans}")

    print(f"Total trip rows validated: {total_trip_rows}, total orphan station refs: {total_orphans}")
    if total_orphans > 0:
        print(
            f"NOTE: {total_orphans} trips reference a station_id not present in the "
            f"stations table (e.g. a decommissioned station) - logged, not a hard failure."
        )

    ti.xcom_push(key="validated_trip_count", value=total_trip_rows)


# ------------------------------------------------------------------
# Load (parallel branches: stations upsert is small, trips uses batch COPY)
# ------------------------------------------------------------------

def load_stations_to_postgres(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import execute_values

    with open(_stations_staging_path()) as f:
        rows = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_destination")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_stations (
            station_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT,
            number_of_docks INTEGER,
            location TEXT,
            synced_at TIMESTAMP DEFAULT now()
        );
        """
    )
    # NOTE: location is stored as the raw "(lat, long)" string from the source
    # for now, matching the map use case's minimum need. Splitting into
    # separate numeric latitude/longitude columns would be the next step if
    # the API needs to do distance/radius queries rather than just display
    # a point on a map.

    records = [
        (r.get("station_id"), r.get("name"), r.get("status"), r.get("number_of_docks"), r.get("location"))
        for r in rows
    ]
    execute_values(
        cur,
        """
        INSERT INTO dim_stations (station_id, name, status, number_of_docks, location)
        VALUES %s
        ON CONFLICT (station_id) DO UPDATE SET
            name = EXCLUDED.name,
            status = EXCLUDED.status,
            number_of_docks = EXCLUDED.number_of_docks,
            location = EXCLUDED.location,
            synced_at = now();
        """,
        records,
    )
    conn.commit()

    # Index created after the (small) load - kept consistent with the
    # trips table's approach, even though the perf difference is
    # negligible at this table's size.
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_stations_name ON dim_stations(name);")
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM dim_stations;")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"Upserted {len(records)} station rows. Table now has {total} rows.")
    context["ti"].xcom_push(key="loaded_station_count", value=total)


def load_trips_to_postgres(**context):
    """
    Batch-loads the fact table using the stage-then-merge pattern:
      1. COPY each batch into an UNLOGGED staging table (fast, no
         indexes, no WAL overhead - this is the actual "batch loading"
         technique, analogous to what's used for large WISSP loads).
      2. A single INSERT ... SELECT ... ON CONFLICT merges staging into
         the real table, handling the upsert/idempotency in one pass.
      3. Indexes are created AFTER the merge, not before - avoids the
         cost of maintaining indexes across hundreds of thousands of
         individual inserts.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ti = context["ti"]
    batch_files = ti.xcom_pull(task_ids="extract_trips_from_bigquery", key="trip_batch_files")

    hook = PostgresHook(postgres_conn_id="postgres_destination")
    conn = hook.get_conn()
    cur = conn.cursor()

    # NOTE on schema: bike_id and the denormalized start_station_name/
    # end_station_name fields are not required by the current data model.
    # Station names are available through the dim_stations join.
    # bike_type is retained for usage analysis by bike type.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_trips (
            trip_id TEXT PRIMARY KEY,
            subscriber_type TEXT,
            bike_type TEXT,
            start_time TIMESTAMP,
            start_station_id TEXT,
            end_station_id TEXT,
            duration_minutes NUMERIC,
            synced_at TIMESTAMP DEFAULT now()
        );
        """
    )
    cur.execute(
        """
        CREATE UNLOGGED TABLE IF NOT EXISTS stg_trips (
            trip_id TEXT,
            subscriber_type TEXT,
            bike_type TEXT,
            start_time TIMESTAMP,
            start_station_id TEXT,
            end_station_id TEXT,
            duration_minutes NUMERIC
        );
        """
    )
    cur.execute("TRUNCATE stg_trips;")
    conn.commit()

    total_copied = 0
    for batch_path in batch_files:
        with open(batch_path) as f:
            rows = json.load(f)
        if not rows:
            continue

        buf = io.StringIO()
        writer = csv.writer(buf)
        for r in rows:
            writer.writerow(
                [
                    r.get("trip_id"),
                    r.get("subscriber_type"),
                    r.get("bike_type"),
                    r.get("start_time"),
                    r.get("start_station_id"),
                    r.get("end_station_id"),
                    r.get("duration_minutes"),
                ]
            )
        buf.seek(0)

        cur.copy_expert(
            """
            COPY stg_trips
                (trip_id, subscriber_type, bike_type, start_time,
                 start_station_id, end_station_id, duration_minutes)
            FROM STDIN WITH CSV
            """,
            buf,
        )
        conn.commit()
        total_copied += len(rows)
        print(f"COPY'd {os.path.basename(batch_path)}: {len(rows)} rows (running total {total_copied})")

    # Merge staging -> real table: one statement, handles the upsert.
    cur.execute(
        """
        INSERT INTO fact_trips
            (trip_id, subscriber_type, bike_type, start_time,
             start_station_id, end_station_id, duration_minutes)
        SELECT trip_id, subscriber_type, bike_type, start_time,
               start_station_id, end_station_id, duration_minutes
        FROM stg_trips
        ON CONFLICT (trip_id) DO UPDATE SET
            subscriber_type = EXCLUDED.subscriber_type,
            bike_type = EXCLUDED.bike_type,
            start_time = EXCLUDED.start_time,
            start_station_id = EXCLUDED.start_station_id,
            end_station_id = EXCLUDED.end_station_id,
            duration_minutes = EXCLUDED.duration_minutes,
            synced_at = now();
        """
    )
    conn.commit()

    # Indexes are created AFTER the bulk merge so they are built once on the
    # loaded data. The subscriber_type index supports queries that group trips
    # by subscriber type.
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_trips_start_station ON fact_trips(start_station_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_trips_end_station ON fact_trips(end_station_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_trips_start_time ON fact_trips(start_time);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_trips_subscriber_type ON fact_trips(subscriber_type);")
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM fact_trips;")
    total_in_table = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"Batch-loaded {total_copied} rows via COPY+merge. Table now has {total_in_table} rows. Indexes created.")
    context["ti"].xcom_push(key="loaded_trip_count", value=total_in_table)


# ------------------------------------------------------------------
# Multi-table JOIN: builds a third table from the two synced tables
# ------------------------------------------------------------------

def create_insights(**context):
    """
    Builds analytical outputs from fact_trips and dim_stations.

    insight_top_stations_by_trips is materialized as an indexed table for
    frequent access. The remaining analytical outputs are views that provide
    different aggregations across stations, subscriber types, time of day,
    day of week, and station capacity.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_destination")
    conn = hook.get_conn()
    cur = conn.cursor()

    # Insight 1 (physical, indexed): busiest stations by trip count.
    cur.execute("DROP TABLE IF EXISTS insight_top_stations_by_trips;")
    cur.execute(
        """
        CREATE TABLE insight_top_stations_by_trips AS
        SELECT
            s.station_id,
            s.name AS station_name,
            COUNT(t.trip_id) AS trip_count,
            AVG(t.duration_minutes) AS avg_duration_minutes
        FROM dim_stations s
        LEFT JOIN fact_trips t
            ON t.start_station_id = s.station_id::text
        GROUP BY s.station_id, s.name;
        """
    )
    cur.execute("ALTER TABLE insight_top_stations_by_trips ADD PRIMARY KEY (station_id);")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_insight_top_stations_trip_count "
        "ON insight_top_stations_by_trips(trip_count DESC);"
    )

    # Insight 2 (view): average trip duration by subscriber type.
    cur.execute("DROP VIEW IF EXISTS insight_avg_duration_by_subscriber_type;")
    cur.execute(
        """
        CREATE VIEW insight_avg_duration_by_subscriber_type AS
        SELECT
            subscriber_type,
            COUNT(*) AS trip_count,
            AVG(duration_minutes) AS avg_duration_minutes
        FROM fact_trips
        GROUP BY subscriber_type
        ORDER BY trip_count DESC;
        """
    )

    # Insight 3 (view): trip volume by hour of day - peak usage times.
    cur.execute("DROP VIEW IF EXISTS insight_trips_by_hour_of_day;")
    cur.execute(
        """
        CREATE VIEW insight_trips_by_hour_of_day AS
        SELECT
            EXTRACT(HOUR FROM start_time) AS hour_of_day,
            COUNT(*) AS trip_count
        FROM fact_trips
        GROUP BY EXTRACT(HOUR FROM start_time)
        ORDER BY hour_of_day;
        """
    )

    # Insight 4 (view): trip volume by day of week.
    cur.execute("DROP VIEW IF EXISTS insight_trips_by_day_of_week;")
    cur.execute(
        """
        CREATE VIEW insight_trips_by_day_of_week AS
        SELECT
            TO_CHAR(start_time, 'Day') AS day_of_week,
            EXTRACT(DOW FROM start_time) AS day_number,
            COUNT(*) AS trip_count
        FROM fact_trips
        GROUP BY TO_CHAR(start_time, 'Day'), EXTRACT(DOW FROM start_time)
        ORDER BY day_number;
        """
    )

    # Insight 5 (view): station utilization - trips per dock, joined with capacity.
    cur.execute("DROP VIEW IF EXISTS insight_station_utilization_ratio;")
    cur.execute(
        """
        CREATE VIEW insight_station_utilization_ratio AS
        SELECT
            s.station_id,
            s.name AS station_name,
            s.number_of_docks,
            COUNT(t.trip_id) AS trip_count,
            ROUND(COUNT(t.trip_id)::numeric / NULLIF(s.number_of_docks, 0), 2) AS trips_per_dock
        FROM dim_stations s
        LEFT JOIN fact_trips t
            ON t.start_station_id = s.station_id::text
        GROUP BY s.station_id, s.name, s.number_of_docks
        ORDER BY trips_per_dock DESC NULLS LAST;
        """
    )

    # Insight 6 (view): electric vs. classic bike usage.
    cur.execute("DROP VIEW IF EXISTS insight_usage_by_bike_type;")
    cur.execute(
        """
        CREATE VIEW insight_usage_by_bike_type AS
        SELECT
            bike_type,
            COUNT(*) AS trip_count,
            AVG(duration_minutes) AS avg_duration_minutes
        FROM fact_trips
        GROUP BY bike_type
        ORDER BY trip_count DESC;
        """
    )

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM insight_top_stations_by_trips;")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(
        f"Built 6 insights: insight_top_stations_by_trips (table, {total} rows), "
        f"insight_avg_duration_by_subscriber_type, insight_trips_by_hour_of_day, "
        f"insight_trips_by_day_of_week, insight_station_utilization_ratio, "
        f"insight_usage_by_bike_type (views)."
    )
    context["ti"].xcom_push(key="summary_row_count", value=total)


# ------------------------------------------------------------------
# Reconciliation
# ------------------------------------------------------------------

def reconcile(**context):
    """
    Since loading is now incremental, comparing "rows extracted this run"
    against "destination total" no longer makes sense after the first run
    (extraction is just this run's delta; destination is cumulative).

    Instead, reconciliation runs a cheap COUNT(*) against the FULL source
    table in BigQuery (fast even at millions of rows - it's a metadata-
    backed aggregate, not a data scan) and compares that to the Postgres
    destination's cumulative total. That's the correct invariant for an
    incrementally-synced fact table: destination should end up matching
    the full source, regardless of how many runs it took to get there.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    ti = context["ti"]
    source_station_count = ti.xcom_pull(task_ids="extract_stations_from_bigquery", key="extracted_station_count")
    summary_count = ti.xcom_pull(task_ids="create_insights", key="summary_row_count")

    bq_hook = BigQueryHook(gcp_conn_id="bigquery_default", use_legacy_sql=False)
    bq_client = bq_hook.get_client()
    bq_total_result = list(
        bq_client.query(
            "SELECT COUNT(*) as cnt FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` "
            "WHERE trip_id IS NOT NULL"
        ).result()
    )
    bq_total_trip_count = bq_total_result[0].cnt

    pg_hook = PostgresHook(postgres_conn_id="postgres_destination")
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM fact_trips;")
    dest_trip_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dim_stations;")
    dest_station_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"Trips: bigquery_full_table_count={bq_total_trip_count}, destination_cumulative={dest_trip_count}")
    print(f"Stations: source={source_station_count}, destination={dest_station_count}")
    print(f"insight_top_stations_by_trips rows: {summary_count} (expected == station count {dest_station_count})")

    try:
        check_reconciliation(bq_total_trip_count, dest_trip_count)
        check_reconciliation(source_station_count, dest_station_count)
    except ValueError as e:
        raise AirflowException(str(e))

    if summary_count != dest_station_count:
        raise AirflowException(
            f"insight_top_stations_by_trips row count ({summary_count}) does not match "
            f"station count ({dest_station_count})."
        )

    print("Reconciliation passed: fact_trips (full-table vs. cumulative destination), dim_stations, and insights.")


def update_watermark(**context):
    """
    Advances the watermark AFTER reconcile has passed - this task only
    runs if everything upstream succeeded, so a failed run never advances
    the watermark and silently loses data on the next incremental pull.
    Uses GREATEST() so the watermark can never move backward even if
    something unexpected happens with extraction ordering.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ti = context["ti"]
    new_watermark = ti.xcom_pull(task_ids="extract_trips_from_bigquery", key="new_watermark")

    if not new_watermark:
        print("No new rows extracted this run - watermark left unchanged.")
        return

    hook = PostgresHook(postgres_conn_id="postgres_destination")
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pipeline_watermark (pipeline_name, last_synced_start_time, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (pipeline_name) DO UPDATE SET
            last_synced_start_time = GREATEST(
                pipeline_watermark.last_synced_start_time, EXCLUDED.last_synced_start_time
            ),
            updated_at = now();
        """,
        (WATERMARK_PIPELINE_NAME, new_watermark),
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Watermark advanced to {new_watermark}.")


# ------------------------------------------------------------------
# DAG assembly
# ------------------------------------------------------------------

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "bikeshare_trips_etl",
    description=(
        "Batch-loaded, incremental (watermark-based) star-schema sync of "
        "Austin Bikeshare data into Postgres: dim_stations + fact_trips, "
        "with 6 insights (1 indexed table + 5 views) built via JOINs."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bikeshare", "etl", "bigquery", "postgres"],
) as dag:

    get_watermark_task = PythonOperator(
        task_id="get_watermark",
        python_callable=get_watermark,
    )

    extract_stations_task = PythonOperator(
        task_id="extract_stations_from_bigquery",
        python_callable=extract_stations_from_bigquery,
    )

    extract_trips_task = PythonOperator(
        task_id="extract_trips_from_bigquery",
        python_callable=extract_trips_from_bigquery,
    )

    validate_task = PythonOperator(
        task_id="validate_extract",
        python_callable=validate_extract,
    )

    load_stations_task = PythonOperator(
        task_id="load_stations_to_postgres",
        python_callable=load_stations_to_postgres,
    )

    load_trips_task = PythonOperator(
        task_id="load_trips_to_postgres",
        python_callable=load_trips_to_postgres,
    )

    create_insights_task = PythonOperator(
        task_id="create_insights",
        python_callable=create_insights,
    )

    reconcile_task = PythonOperator(
        task_id="reconcile",
        python_callable=reconcile,
    )

    update_watermark_task = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark,
    )

    # get_watermark feeds extract_trips (needs to know where to resume from).
    # extract_stations has no watermark dependency - dimension table, full reload every run.
    # Both extracts must finish before validate (which checks referential integrity
    # between them). Loads run in parallel, then join -> reconcile -> only THEN
    # advance the watermark, so a failed run never loses data on the next incremental pull.
    get_watermark_task >> extract_trips_task
    [extract_stations_task, extract_trips_task] >> validate_task
    validate_task >> [load_stations_task, load_trips_task] >> create_insights_task
    create_insights_task >> reconcile_task >> update_watermark_task