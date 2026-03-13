from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue

import requests
import pendulum

from helpers.logger import logger
from f1.tasks import (
    _get_car_data, _get_drivers, _get_intervals, _get_locations,
    _get_meetings, _get_specific_meeting, _get_pits, _get_positions,
    _get_race_control, _get_sessions, _get_stints, _get_team_radio,
    _store_sessions, _get_session_list, _get_driver_list, _get_laps,
)

BUCKET_NAME = 'formula1'


@dag(
    start_date=pendulum.datetime(2025, 1, 1),
    description='Scrape data from open F1 API',
    default_args={
        'retries': 1,
    },
    catchup=False,
    tags=['formula1', 'lab-ariflow', 'raw'],
    max_active_runs=4,
    max_active_tasks=3,
    max_consecutive_failed_dag_runs=2,
)
def backfill_meeting():
    """
    Manually triggered DAG to backfill a specific F1 meeting by key, name, or location.

    Trigger with dag_run.conf, e.g.:
        {"meeting_key": "1261", "year_date": "2024-03-15"}
    or:
        {"meeting_name": "Bahrain Grand Prix", "year_date": "2024-03-15"}

    Dependencies:
        Airflow Connection:
            F1_BASE_API: This is the airflow connection.
            POSTGRES_DW: Postgres datawarehouse airflow connection
            MINIO: Minio client connection

        Infra:
            MINIO: Object storage [infra]
            POSTGRES DW: Postgres Data warehouse [infra]
    """

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        base_api = BaseHook.get_connection('f1_base_api')
        url = f"{base_api.host}/meetings?year={pendulum.now().year}"
        logger.info(f"Complete URL: {url}")
        response = requests.get(url, headers=base_api.extra_dejson['headers'])
        logger.info(f"Response code: {response.status_code}")
        return PokeReturnValue(is_done=response.status_code == 200, xcom_value=base_api.host)

    @task()
    def get_meetings():
        # airflow.operators.python may not resolve in local venv — works fine at runtime
        from airflow.operators.python import get_current_context  # noqa: PLC0415
        context = get_current_context()
        conf = context['dag_run'].conf or {}
        year_date = conf.get('year_date') or context['ds']
        return _get_meetings(year_date)

    @task()
    def get_specific_meeting(data):
        from airflow.operators.python import get_current_context  # noqa: PLC0415
        conf = get_current_context()['dag_run'].conf or {}
        return _get_specific_meeting(
            data,
            meeting_key=conf.get('meeting_key'),
            meeting_name=conf.get('meeting_name'),
            meeting_location=conf.get('meeting_location'),
        )

    @task()
    def get_sessions(meeting_key):
        return _get_sessions(meeting_key)

    @task()
    def store_sessions(meeting_key, data):
        return _store_sessions(meeting_key, data)

    @task()
    def get_drivers(meeting_key):
        return _get_drivers(meeting_key)

    @task()
    def get_stints(meeting_key):
        return _get_stints(meeting_key)

    @task()
    def get_team_radio(meeting_key):
        return _get_team_radio(meeting_key)

    @task()
    def get_pits(meeting_key):
        return _get_pits(meeting_key)

    @task()
    def get_race_control(meeting_key):
        return _get_race_control(meeting_key)

    @task()
    def get_session_list(sessions):
        return _get_session_list(sessions)

    @task()
    def get_driver_list(session_keys_list, drivers_asset_path):
        return _get_driver_list(session_keys_list, drivers_asset_path)

    @task(max_active_tis_per_dag=3)
    def fetch_position_data(meeting_key, session_key, driver_number):
        return _get_positions(meeting_key, session_key, driver_number)

    @task(max_active_tis_per_dag=3)
    def fetch_location_data(meeting_key, session_key, driver_number):
        return _get_locations(meeting_key, session_key, driver_number)

    @task(max_active_tis_per_dag=3)
    def fetch_car_data(meeting_key, session_key, driver_number):
        return _get_car_data(meeting_key, session_key, driver_number)

    @task(max_active_tis_per_dag=3)
    def fetch_interval_data(meeting_key, session_key, driver_number):
        return _get_intervals(meeting_key, session_key, driver_number)

    @task(max_active_tis_per_dag=3)
    def fetch_lap_data(meeting_key, session_key, driver_number):
        return _get_laps(meeting_key, session_key, driver_number)

    # Wire up the DAG
    base_api = is_api_available()
    meetings = get_meetings()
    base_api >> meetings  # explicit dependency: sensor must pass before fetching meetings

    meeting_key = get_specific_meeting(meetings)

    sessions = get_sessions(meeting_key)
    store_sessions(meeting_key, sessions)

    drivers_path = get_drivers(meeting_key)
    get_stints(meeting_key)
    get_team_radio(meeting_key)
    get_pits(meeting_key)
    get_race_control(meeting_key)

    session_list = get_session_list(sessions)
    driver_list = get_driver_list(session_list, drivers_path)

    fetch_position_data.expand_kwargs(driver_list)
    fetch_location_data.expand_kwargs(driver_list)
    fetch_car_data.expand_kwargs(driver_list)
    fetch_interval_data.expand_kwargs(driver_list)
    fetch_lap_data.expand_kwargs(driver_list)


backfill_meeting()
