from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator 
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack import SlackNotifier
from datetime import datetime
from airflow.sdk import Asset
from airflow.datasets import Dataset

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, _load_csv_to_postgres

SYMBOL='NVDA'

dw_stock_table = Asset(name="dw_stock_table", uri="postgresql://postgres:5432/postgres/public/stock_market")

# from airflow.providers.postgres.assets.postgres.

@dag(
    start_date=datetime(2025, 4, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'], 
    on_success_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market ran succesfully',
        # channel='general'
        channel='airflow-monitoring'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market failed',
        channel='general'
    )
)
def stock_market():

    @task.sensor(poke_interval=30, timeout=300 , mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests

        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)

        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition  = response.json()['finance']['result']  is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id = 'get_stock_prices',
        python_callable = _get_stock_prices,
        op_kwargs = {'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}' , 'symbol': SYMBOL}
    )

    store_prices = PythonOperator(
        task_id = 'store_prices',
        python_callable = _store_prices,
        op_kwargs = {'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )

    format_prices = DockerOperator(
        task_id = 'format_prices',
        image = 'airflow/stock-app',
        container_name = 'format_prices',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375', # referencing the docker-proxy service in the docker compose/override file
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices")}}' # will override default SPARK_APPLICATION_ARGS
        }
    )

    get_formatted_csv = PythonOperator(
        task_id = 'get_formatted_csv',
        python_callable= _get_formatted_csv,
        op_kwargs= {
            'path': '{{ ti.xcom_pull(task_ids="store_prices")}}' # format -> {bucket_name}/{symbol}  e.g stock-market/NVDA
        }
    )

    load_to_dw = PythonOperator(
        task_id = 'load_to_dw',
        python_callable = _load_csv_to_postgres,
        op_kwargs= {
            'filepath': '{{ti.xcom_pull(task_ids="get_formatted_csv")}}'
        },
        outlets=[dw_stock_table]
    )
    # when using the task decorator, you have the call the task function
    # eg is_api_available() instead of is_api_availiable     
    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw

stock_market()