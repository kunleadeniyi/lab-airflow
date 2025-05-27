from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import json
import pandas as pd

import http.client as http_client
import logging

http_client.HTTPConnection.debuglevel = 1

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

BUCKET_NAME = 'stock-market'

def _get_minio_client():
    # get minio client
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.host.split('//')[1],
        access_key=minio.login,
        secret_key=minio.password, 
        secure=False
    )
    return client

def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?interval=1d&range=1Y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])

    if response.status_code != 200:
        logging.error(f"API Call Status code: {response.status_code} \nReponse: {response}")

    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    # get minio client
    client = _get_minio_client()

    # create bucket if bucket does not exist
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    
    objw = client.put_object(
        bucket_name=BUCKET_NAME, 
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'

def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix = f"{path.split('/')[1]}/formatted_prices/"

    objects = client.list_objects(bucket_name=BUCKET_NAME, prefix=prefix, recursive=True)

    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
        
    return AirflowNotFoundException('CSV file does not exist')

def _load_csv_to_postgres(filepath, table_name='stock_market'):
    minio_client = _get_minio_client()

    # filepath example - NVDA/formatted_prices/part-00000-98f88765-c3bd-4f40-8027-23124f010b17-c000.csv
    data = minio_client.get_object(bucket_name=BUCKET_NAME, object_name=filepath)
    # Path to your CSV
    table_name = table_name

    # Load CSV with pandas
    df = pd.read_csv(data)

    # Connect to Postgres
    hook = PostgresHook(postgres_conn_id='postgres')
    engine = hook.get_sqlalchemy_engine()

    # Insert data (replace table or append)
    df.to_sql(table_name, engine, if_exists='replace', index=False)