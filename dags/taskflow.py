from airflow.decorators import dag, task
from pendulum import datetime
import requests


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["taskflow"],
)
def taskflow():
    # Define tasks
    @task() 
    def task_a():
        print('Task A')
        return 42
    
    @task
    def task_b(value):
        print("Task B")
        print(value)

    task_b(task_a())

taskflow()