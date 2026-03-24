"""
BigQuery F1 bronze load DAG.

Reads raw JSON files from MinIO and loads them into BigQuery bronze.*
tables via load jobs (NDJSON, WRITE_APPEND). This DAG is intentionally
decoupled from the MinIO ingestion DAGs (f1_dag, backfill_specific_f1_meeting).
MinIO is the source of truth; this DAG is a downstream consumer with its
own retry/failure boundary.

Trigger:
    Manually via the Airflow UI, or via TriggerDagRunOperator from f1_dag
    once ingestion completes.

Required environment variables (set in docker-compose.yaml):
    GOOGLE_APPLICATION_CREDENTIALS — path to the GCP service account JSON key
    GCP_PROJECT                    — GCP project ID (default: dbt-airflow-project-f1)

Required Airflow connection:
    minio — MinIO object storage (host, port, login, password)

Conf parameters:
    year         (str, required) — e.g. "2025"
    meeting_key  (str, optional) — e.g. "1262"
                  Omit to load all meetings for the year (full backfill).

All 12 entity load tasks run in parallel once parameters are resolved.
"""

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

log = logging.getLogger(__name__)

from f1.bigquery_load_tasks import (
    _load_car_data,
    _load_drivers,
    _load_intervals,
    _load_laps,
    _load_locations,
    _load_meetings,
    _load_pits,
    _load_positions,
    _load_race_control,
    _load_sessions,
    _load_stints,
    _load_team_radio,
)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=['formula1', 'bigquery', 'load'],
    default_args={
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=2),
    },
    max_active_runs=1,
    max_active_tasks=4,
    doc_md=__doc__,
)
def bigquery_f1_load():

    @task()
    def get_year():
        context = get_current_context()
        conf = context['dag_run'].conf or {}
        year = conf.get('year') or str(
            pendulum.from_format(context['ds'], fmt='YYYY-MM-DD').year
        )
        log.info("get_year resolved: value=%r type=%s", year, type(year).__name__)
        return year

    @task()
    def get_meeting_key():
        context = get_current_context()
        conf = context['dag_run'].conf or {}
        meeting_key = conf.get('meeting_key')  # None means load all meetings for the year
        log.info("get_meeting_key resolved: value=%r type=%s", meeting_key, type(meeting_key).__name__)
        return meeting_key

    def _log_params(task_name, year, meeting_key):
        log.info(
            "%s received: year=%r (type=%s), meeting_key=%r (type=%s)",
            task_name, year, type(year).__name__, meeting_key, type(meeting_key).__name__,
        )

    # -- meeting-level entities --

    @task()
    def load_meetings(year, meeting_key):
        _log_params('load_meetings', year, meeting_key)
        _load_meetings(year, meeting_key)

    @task()
    def load_sessions(year, meeting_key):
        _log_params('load_sessions', year, meeting_key)
        _load_sessions(year, meeting_key)

    @task()
    def load_drivers(year, meeting_key):
        _log_params('load_drivers', year, meeting_key)
        _load_drivers(year, meeting_key)

    @task()
    def load_pits(year, meeting_key):
        _log_params('load_pits', year, meeting_key)
        _load_pits(year, meeting_key)

    @task()
    def load_stints(year, meeting_key):
        _log_params('load_stints', year, meeting_key)
        _load_stints(year, meeting_key)

    @task()
    def load_race_control(year, meeting_key):
        _log_params('load_race_control', year, meeting_key)
        _load_race_control(year, meeting_key)

    @task()
    def load_team_radio(year, meeting_key):
        _log_params('load_team_radio', year, meeting_key)
        _load_team_radio(year, meeting_key)

    # -- session+driver-level entities --

    @task()
    def load_laps(year, meeting_key):
        _log_params('load_laps', year, meeting_key)
        _load_laps(year, meeting_key)

    @task()
    def load_positions(year, meeting_key):
        _log_params('load_positions', year, meeting_key)
        _load_positions(year, meeting_key)

    @task()
    def load_intervals(year, meeting_key):
        _log_params('load_intervals', year, meeting_key)
        _load_intervals(year, meeting_key)

    @task()
    def load_car_data(year, meeting_key):
        _log_params('load_car_data', year, meeting_key)
        _load_car_data(year, meeting_key)

    @task()
    def load_locations(year, meeting_key):
        _log_params('load_locations', year, meeting_key)
        _load_locations(year, meeting_key)

    # -- wire: all load tasks fan out in parallel from resolved scalar XComs --

    year = get_year()
    meeting_key = get_meeting_key()

    load_meetings(year, meeting_key)
    load_sessions(year, meeting_key)
    load_drivers(year, meeting_key)
    # load_pits(year, meeting_key)
    # load_stints(year, meeting_key)
    # load_race_control(year, meeting_key)
    # load_team_radio(year, meeting_key)
    # load_laps(year, meeting_key)
    # load_positions(year, meeting_key)
    # load_intervals(year, meeting_key)
    # load_car_data(year, meeting_key)
    # load_locations(year, meeting_key)


bigquery_f1_load()
