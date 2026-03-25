from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pendulum

from f1.tasks import _get_meetings, _get_most_recent_meeting, _store_meetings
from f1.shared_tasks import is_api_available, wire_f1_pipeline


@dag(
    start_date=pendulum.datetime(2025, 1, 1),
    description='Scrape data from open F1 API',
    schedule='@weekly',
    default_args={
        'retries': 1,
    },
    catchup=False,
    tags=['formula1', 'lab-ariflow', 'raw'],
    max_active_runs=4,
    max_active_tasks=16,
    max_consecutive_failed_dag_runs=2,
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

    @task()
    def get_year():
        from airflow.operators.python import get_current_context  # noqa: PLC0415
        import pendulum as _pendulum
        ds = get_current_context()['ds']
        return str(_pendulum.from_format(ds, fmt='YYYY-MM-DD').year)

    @task()
    def get_meetings(year):
        return _get_meetings(year)

    @task()
    def store_meetings(data, year):
        return _store_meetings(data, year)

    @task()
    def get_most_recent_meeting(data):
        return _get_most_recent_meeting(data)

    base_api = is_api_available()
    year = get_year()
    meetings = get_meetings(year)
    base_api >> meetings  # sensor must pass before fetching meetings

    store_meetings(meetings, year)
    meeting_key = get_most_recent_meeting(meetings)
    last_tasks = wire_f1_pipeline(meeting_key, year)

    trigger = TriggerDagRunOperator(
        task_id='trigger_bigquery_load',
        trigger_dag_id='bigquery_f1_load',
        conf={
            'year': "{{ ti.xcom_pull(task_ids='get_year') }}",
            'meeting_key': "{{ ti.xcom_pull(task_ids='get_most_recent_meeting') }}",
        },
        wait_for_completion=False,
    )
    for t in last_tasks:
        t >> trigger


f1()
