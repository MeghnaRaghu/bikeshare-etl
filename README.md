# Austin Bikeshare ETL

An Apache Airflow ETL pipeline that extracts Austin Bikeshare data from BigQuery, validates and batch-loads it into PostgreSQL, and creates analytical outputs for downstream querying.

## Architecture

```text
BigQuery
   |
   | Paginated extraction
   v
Airflow DAG
   |
   | Validation + batch processing
   v
PostgreSQL staging
   |
   | COPY + merge/upsert
   v
PostgreSQL
   |
   +--> dim_stations
   +--> fact_trips
   +--> Analytical insight outputs
```

## Pipeline Features

- Paginated extraction from BigQuery
- Batch loading into PostgreSQL using COPY
- Staging-table based loading
- Fact and dimension data model
- Incremental processing using a pipeline watermark
- Station and trip data validation
- Referential-integrity checks
- Source-to-target reconciliation
- Idempotent merge/upsert processing
- PostgreSQL indexes created after bulk loading
- Analytical outputs using SQL joins and aggregations
- Configurable batch size and staging directory through Airflow Variables
- Unit-tested helper functions

## Data Model

### dim_stations

Contains station-level information:

- station_id
- name
- status
- number_of_docks
- location

### fact_trips

Contains trip-level information:

- trip_id
- subscriber_type
- bike_type
- start_time
- start_station_id
- end_station_id
- duration_minutes

Station attributes are maintained in `dim_stations` and referenced by station identifiers in `fact_trips`.

## Analytical Outputs

The pipeline generates analytical outputs covering:

- Station trip volumes
- Average trip duration by subscriber type
- Trips by hour
- Trips by day of week
- Station utilization and capacity
- Usage by bike type

## Performance

The pipeline is designed for large-volume processing through:

- Paginated BigQuery extraction
- Batch-based PostgreSQL loading
- PostgreSQL COPY
- Bulk loading before index creation
- Incremental processing with a watermark
- Indexed query patterns

## Testing

Run the helper test suite:

```bash
pytest -q tests/test_bikeshare_helpers.py
```

Current result:

```text
29 passed
```

## Project Structure

```text
bikeshare-etl/
├── dags/
│   ├── bikeshare_trips_etl.py
│   └── sync_helpers.py
├── tests/
│   └── test_bikeshare_helpers.py
├── conftest.py
├── docker-compose.yaml
├── .gitignore
└── README.md
```

## Technologies

- Python
- Apache Airflow
- Google BigQuery
- PostgreSQL
- SQL
- Pytest
- Docker
