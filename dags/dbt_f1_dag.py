from pathlib import Path

import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode

DBT_DIR = Path("/opt/airflow/dbt")

project_config = ProjectConfig(
    dbt_project_path=DBT_DIR,
)

profile_config = ProfileConfig(
    profile_name="f1_warehouse",
    target_name="dev",
    profiles_yml_filepath=DBT_DIR / "profiles" / "profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=Path("/home/airflow/.local/bin/dbt"),
)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["formula1", "dbt"],
    max_active_runs=1,
)
def dbt_f1():
    DbtTaskGroup(
        group_id="f1_models",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,  # parses project live via `dbt ls`
        ),
    )


dbt_f1()
