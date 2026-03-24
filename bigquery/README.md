# BigQuery Initialisation

Scripts and DDL for setting up the BigQuery bronze layer.

## Prerequisites

- Python 3.10+
- A GCP service account key with the following roles:
  - `roles/bigquery.dataEditor` — create/write tables
  - `roles/bigquery.jobUser` — run load jobs and DDL queries
- The `google-cloud-bigquery` package installed
- BigQuery datasets `bronze`, `silver`, and `gold` already created in project `dbt-airflow-project-f1`

## Setup

Install dependencies (run from project root):

```bash
pip install google-cloud-bigquery
```

Place the service account key at the project root as `dbt-airflow-sa.json`, or set the environment variable explicitly:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/key.json
```

## Running the schema initialisation

From the **project root**:

```bash
python bigquery/init/apply_bronze_schema.py
```

The script will:
1. Read `bigquery/init/01_bronze_tables.sql`
2. Split it into individual `CREATE TABLE IF NOT EXISTS` statements
3. Execute each statement against BigQuery project `dbt-airflow-project-f1`
4. Print `OK` for each table on success

All statements use `IF NOT EXISTS`, so re-running the script is safe — existing tables are not modified or dropped.

### Expected output

```
Applying 12 DDL statements to project dbt-airflow-project-f1...

  Running: CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.meetings`...
  OK

  Running: CREATE TABLE IF NOT EXISTS `dbt-airflow-project-f1.bronze.sessions`...
  OK

  ...

All bronze tables created successfully.
```

## Schema overview

See [init/01_bronze_tables.sql](init/01_bronze_tables.sql) for the full DDL.

| Table | Partitioning | Clustering |
|---|---|---|
| `meetings` | none | `meeting_key` |
| `sessions` | none | `meeting_key, session_key` |
| `drivers` | none | `meeting_key, session_key, driver_number` |
| `stints` | none | `meeting_key, session_key, driver_number, stint_number` |
| `laps` | `DATE(_loaded_at)` | `meeting_key, session_key, driver_number` |
| `pits` | `DATE(date)` | `meeting_key, session_key, driver_number` |
| `positions` | `DATE(date)` | `meeting_key, session_key, driver_number` |
| `intervals` | `DATE(date)` | `meeting_key, session_key, driver_number` |
| `car_data` | `DATE(date)` | `meeting_key, session_key, driver_number` |
| `locations` | `DATE(date)` | `meeting_key, session_key, driver_number` |
| `race_control` | `DATE(date)` | `meeting_key, session_key` |
| `team_radio` | `DATE(date)` | `meeting_key, session_key, driver_number` |

Partitioning rationale is documented in [../docs/decision_log.md](../docs/decision_log.md) — ADR-001.
