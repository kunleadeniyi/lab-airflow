from airflow.decorators import dag, task

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
    def get_meetings():
        # airflow.operators.python may not resolve in local venv — works fine at runtime
        from airflow.operators.python import get_current_context  # noqa: PLC0415
        ds = get_current_context()['ds']
        return _get_meetings(ds)

    @task()
    def store_meetings(data):
        return _store_meetings(data)

    @task()
    def get_most_recent_meeting(data):
        return _get_most_recent_meeting(data)

    base_api = is_api_available()
    meetings = get_meetings()
    base_api >> meetings  # explicit dependency: sensor must pass before fetching meetings
    store_meetings(meetings)

    meeting_key = get_most_recent_meeting(meetings)
    wire_f1_pipeline(meeting_key)


f1()
