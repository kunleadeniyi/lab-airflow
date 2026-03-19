import clickhouse_connect
from airflow.hooks.base import BaseHook

from helpers.logger import logger


def get_clickhouse_client():
    conn = BaseHook.get_connection('clickhouse_dw')
    logger.info(f"Connecting to ClickHouse at {conn.host}:{conn.port or 8123} as {conn.login}")
    return clickhouse_connect.get_client(
        host=conn.host,
        port=int(conn.port or 8123),
        username=conn.login,
        password=conn.password,
    )
