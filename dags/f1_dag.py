from airflow import DAG
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel
from airflow.models.xcom_arg import XComArg
from airflow.utils.timezone import make_aware

import requests
import pendulum
import json
import time
from minio.helpers import ObjectWriteResult

# custom 
from helpers.logger import logger
from helpers.minio import get_minio_client
from f1.tasks import _get_car_data, _get_drivers, _get_intervals, _get_locations, _get_meetings, _get_most_recent_meeting, _get_pits, _get_positions, _get_race_control, _get_sessions, _get_stints, _get_team_radio, _get_weather, _store_data, _store_meetings, _store_sessions, _get_session_list, _get_driver_list, _get_laps

BUCKET_NAME='formula1'

def print_args(x, y):
    print(x)
    print(y)
    return x + y
## helper funtions above to be moved to another file in the include folder
@dag(
    start_date=pendulum.datetime(2025, 1, 1),
    description='Scrape data from open F1 API',
    schedule='@weekly',
    default_args={
        'retries': 1, #2,
    },
    catchup=False,
    tags=['formula1', 'lab-ariflow', 'raw'],
    max_active_runs=4,
    max_active_tasks=16,
    max_consecutive_failed_dag_runs=2,
    # on_failure_callback= ,
    # on_success_callback= ,
)
def f1():
    """
    Dependencies:
        Airflow Connection:
            F1_BASE_API: This is the airflow connection.
            POSTGRES_DW: Postgres datawarehouse airflow connection
            MINIO: Minio client connection

        Infra:
            MINIO: Object storage [infra]
            POSTGRES DW: Postgres Data warehouse [infra]
    """
    @task.sensor(poke_interval=30, timeout=300 , mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests

        base_api = BaseHook.get_connection('f1_base_api')
        url = f"{base_api.host}/meetings?year={pendulum.now().year}"
        logger.info(f"Complete URL: {url}")

        response = requests.get(url, headers=base_api.extra_dejson['headers'])
        logger.info(f"Response code: {response.status_code}")
        condition  = response.status_code == 200
        return PokeReturnValue(is_done=condition, xcom_value=base_api.host)
    
    @task()
    def test_task():
        print("This is a test task")

    get_meetings = PythonOperator(
        task_id = 'get_meetings',
        python_callable = _get_meetings,
        op_kwargs = {'base_api': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'ds': '{{ ds }}'}
    )

    # creates assets -> formula1/meetings/meetings.json
    store_meetings = PythonOperator(
        task_id = 'store_meetings',
        python_callable = _store_meetings,
        op_kwargs = {'data': '{{ ti.xcom_pull(task_ids="get_meetings") }}'}
    )

    get_most_recent_meeting = PythonOperator(
        task_id = 'get_most_recent_meeting',
        python_callable= _get_most_recent_meeting,
        op_kwargs= {'data': '{{ ti.xcom_pull(task_ids="get_meetings") }}'}
    )

    # skip all downstream task if meeting key is not valid, i.e, if  meeting key from upstream task is 0000
    get_sessions = PythonOperator(
        task_id = 'get_sessions',
        python_callable= _get_sessions,
        op_kwargs={
            'meeting_key': '{{ ti.xcom_pull(task_ids="get_most_recent_meeting") }}'
        }
    )

    # creates assets -> formula1/sessions/{meeting_key}/session.json
    store_sessions = PythonOperator(
        task_id = 'store_sessions',
        python_callable = _store_sessions,
        op_kwargs={
            'meeting_key': '{{ ti.xcom_pull(task_ids="get_most_recent_meeting") }}',
            'data': '{{ ti.xcom_pull(task_ids="get_sessions") }}'
        }
    )

    get_drivers = PythonOperator(
        task_id = "get_drivers",
        python_callable = _get_drivers,
        op_kwargs={
            'meeting_key': '{{ ti.xcom_pull(task_ids="get_most_recent_meeting") }}',
        }
    )

    get_stints = PythonOperator(
        task_id = "get_stints",
        python_callable = _get_stints,
        op_kwargs={
            'meeting_key': '{{ ti.xcom_pull(task_ids="get_most_recent_meeting") }}',
        }
    )
    
    get_team_radio = PythonOperator(
        task_id = "get_team_radio",
        python_callable = _get_team_radio,
        op_kwargs={
            'meeting_key': '{{ ti.xcom_pull(task_ids="get_most_recent_meeting") }}',
        }
    )

    get_pits = PythonOperator(
        task_id = "get_pits",
        python_callable = _get_pits,
        op_kwargs={
            'meeting_key': '{{ ti.xcom_pull(task_ids="get_most_recent_meeting") }}',
        }
    )

    get_race_control = PythonOperator(
        task_id = "get_race_control",
        python_callable = _get_race_control,
        op_kwargs={
            'meeting_key': '{{ ti.xcom_pull(task_ids="get_most_recent_meeting") }}',
        }
    )

    get_session_list = PythonOperator(
        task_id = 'get_session_list',
        python_callable = _get_session_list,
        op_kwargs={
             'sessions': '{{ ti.xcom_pull(task_ids="get_sessions") }}'
        }
    )

    get_driver_list = PythonOperator(
        task_id = 'get_driver_list',
        python_callable= _get_driver_list,
        op_kwargs= {
            # session_keys_list, drivers_asset_path
            'session_keys_list': '{{ ti.xcom_pull(task_ids="get_session_list") }}',
            'drivers_asset_path': '{{ ti.xcom_pull(task_ids="get_drivers") }}'
        }
    )

    # expansion_example = PythonOperator.partial(task_id="task-1", python_callable=print_args).expand_kwargs(
    #     [
    #         {"op_kwargs": {"x": 1, "y": 2}, "show_return_value_in_logs": True},
    #         {"op_kwargs": {"x": 3, "y": 4}, "show_return_value_in_logs": False},
    #     ]
    # )

    fetch_positions_data = PythonOperator.partial(
        task_id='fetch_position_data',
        python_callable=_get_positions,
        max_active_tis_per_dag=3
    ).expand(op_args=get_driver_list.output)

    fetch_location_data = PythonOperator.partial(
        task_id='fetch_location_data',
        python_callable=_get_locations,
        max_active_tis_per_dag=3
    ).expand(op_args=get_driver_list.output)

    fetch_car_data = PythonOperator.partial(
        task_id='fetch_car_data',
        python_callable=_get_car_data,
        max_active_tis_per_dag=3
    ).expand(op_args=get_driver_list.output)

    fetch_interval_data = PythonOperator.partial(
        task_id='fetch_interval_data',
        python_callable=_get_intervals,
        max_active_tis_per_dag=3
    ).expand(op_args=get_driver_list.output)

    fetch_lap_data = PythonOperator.partial(
        task_id='fetch_lap_data',
        python_callable=_get_laps,
        max_active_tis_per_dag=3
    ).expand(op_args=get_driver_list.output)

    test_task() >> is_api_available() >> get_meetings >> store_meetings
    get_meetings >> get_most_recent_meeting
    get_most_recent_meeting >> [get_sessions, get_drivers, get_stints, get_team_radio, get_pits, get_race_control]
    get_sessions >> store_sessions 
    get_sessions >> get_session_list 
    [get_session_list, get_drivers] >> get_driver_list >> [fetch_positions_data, fetch_location_data, fetch_car_data, fetch_interval_data, fetch_lap_data]
   

f1()

