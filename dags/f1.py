from airflow import DAG
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator

import requests
import pendulum
import json

# make it a standard logger and import instead
# will seat in the include folder too 
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

## Helper functions
def _get_meetings(base_api, ds):
    logging.info(f"date is {ds}")
    url = f"{base_api}/meetings?year={pendulum.from_format(ds, fmt='YYYY-MM-DD').year}"
    api = BaseHook.get_connection('f1_base_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])

    if response.status_code != 200:
        logging.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    return json.dumps(response.json())


## helper funtions above to be moved to another file in the include folder
@dag(
    start_date=pendulum.datetime(2025, 1, 1),
    description='Scrape data from open F1 API',
    schedule='@weekly',
    default_args={
        'retries': 2,
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
            MINIO: Minion client connection

        Infra:
            MINIO: Object storage [infra]
            POSTGRES DW: Postgres Data warehouse [infra]
    """
    @task.sensor(poke_interval=30, timeout=300 , mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests

        base_api = BaseHook.get_connection('f1_base_api')
        url = f"{base_api.host}/meetings?year={pendulum.now().year}"
        logging.info(f"Complete URL: {url}")

        response = requests.get(url, headers=base_api.extra_dejson['headers'])
        logging.info(f"Response code: {response.status_code}")
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

    test_task() >> is_api_available() >> get_meetings

f1()
