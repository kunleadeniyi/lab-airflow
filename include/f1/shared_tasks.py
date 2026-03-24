from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue

import requests
import pendulum

from helpers.logger import logger
from f1.tasks import (
    _get_car_data, _get_drivers, _get_intervals, _get_locations,
    _get_pits, _get_positions, _get_race_control, _get_sessions,
    _get_stints, _get_team_radio, _store_sessions,
    _get_session_list, _get_driver_list, _get_laps,
)


@task.sensor(poke_interval=30, timeout=300, mode='poke')
def is_api_available() -> PokeReturnValue:
    base_api = BaseHook.get_connection('f1_base_api')
    url = f"{base_api.host}/meetings?year={pendulum.now().year}"
    logger.info(f"Complete URL: {url}")
    response = requests.get(url, headers=base_api.extra_dejson['headers'])
    logger.info(f"Response code: {response.status_code}")
    return PokeReturnValue(is_done=response.status_code == 200, xcom_value=base_api.host)


@task()
def get_sessions(meeting_key):
    return _get_sessions(meeting_key)


@task()
def store_sessions(meeting_key, data, year):
    return _store_sessions(meeting_key, data, year)


@task()
def get_drivers(meeting_key, year):
    return _get_drivers(meeting_key, year)


@task()
def get_stints(meeting_key, year):
    return _get_stints(meeting_key, year)


@task()
def get_team_radio(meeting_key, year):
    return _get_team_radio(meeting_key, year)


@task()
def get_pits(meeting_key, year):
    return _get_pits(meeting_key, year)


@task()
def get_race_control(meeting_key, year):
    return _get_race_control(meeting_key, year)


@task()
def get_session_list(sessions):
    return _get_session_list(sessions)


@task()
def get_driver_list(session_keys_list, driver_data, year):
    return _get_driver_list(session_keys_list, driver_data, year)


@task(max_active_tis_per_dag=3)
def fetch_position_data(meeting_key, session_key, driver_number, year):
    return _get_positions(meeting_key, session_key, driver_number, year)


@task(max_active_tis_per_dag=3)
def fetch_location_data(meeting_key, session_key, driver_number, year):
    return _get_locations(meeting_key, session_key, driver_number, year)


@task(max_active_tis_per_dag=3)
def fetch_car_data(meeting_key, session_key, driver_number, year):
    return _get_car_data(meeting_key, session_key, driver_number, year)


@task(max_active_tis_per_dag=3)
def fetch_interval_data(meeting_key, session_key, driver_number, year):
    return _get_intervals(meeting_key, session_key, driver_number, year)


@task(max_active_tis_per_dag=3)
def fetch_lap_data(meeting_key, session_key, driver_number, year):
    return _get_laps(meeting_key, session_key, driver_number, year)


def wire_f1_pipeline(meeting_key, year):
    """
    Wire the shared downstream F1 pipeline given a meeting_key and year XComArgs.
    Must be called inside an active @dag context.

    Returns the list of terminal task instances so callers can chain
    downstream tasks (e.g. TriggerDagRunOperator) after all writes complete.
    """
    sessions = get_sessions(meeting_key)
    stored_sessions = store_sessions(meeting_key, sessions, year)

    driver_data = get_drivers(meeting_key, year)
    stints = get_stints(meeting_key, year)
    radio = get_team_radio(meeting_key, year)
    pits = get_pits(meeting_key, year)
    race_ctrl = get_race_control(meeting_key, year)

    session_list = get_session_list(sessions)
    driver_list = get_driver_list(session_list, driver_data, year)

    positions = fetch_position_data.expand_kwargs(driver_list)
    locations = fetch_location_data.expand_kwargs(driver_list)
    car_data = fetch_car_data.expand_kwargs(driver_list)
    intervals = fetch_interval_data.expand_kwargs(driver_list)
    laps = fetch_lap_data.expand_kwargs(driver_list)

    return [stored_sessions, stints, radio, pits, race_ctrl, positions, locations, car_data, intervals, laps]
