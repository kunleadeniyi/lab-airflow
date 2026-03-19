from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pendulum

from f1.tasks import _get_meetings, _get_specific_meeting
from f1.shared_tasks import is_api_available, wire_f1_pipeline


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

    @task()
    def get_year():
        from airflow.operators.python import get_current_context  # noqa: PLC0415
        import pendulum as _pendulum
        context = get_current_context()
        conf = context['dag_run'].conf or {}
        year_date = conf.get('year_date') or context['ds']
        return str(_pendulum.from_format(year_date, fmt='YYYY-MM-DD').year)

    @task()
    def get_meetings(year):
        return _get_meetings(year)

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

    base_api = is_api_available()
    year = get_year()
    meetings = get_meetings(year)
    base_api >> meetings  # sensor must pass before fetching meetings

    meeting_key = get_specific_meeting(meetings)
    last_tasks = wire_f1_pipeline(meeting_key, year)

    trigger = TriggerDagRunOperator(
        task_id='trigger_clickhouse_load',
        trigger_dag_id='clickhouse_f1_load',
        conf={
            'year': "{{ ti.xcom_pull(task_ids='get_year') }}",
            'meeting_key': "{{ ti.xcom_pull(task_ids='get_specific_meeting') }}",
        },
        wait_for_completion=False,
    )
    for t in last_tasks:
        t >> trigger


backfill_meeting()
