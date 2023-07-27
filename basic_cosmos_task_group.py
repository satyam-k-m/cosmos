"""
An example DAG that uses Cosmos to render a dbt project as a TaskGroup.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig,LoadMode
# from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


# DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
# DEFAULT_DBT_ROOT_PATH = "/opt/airflow/git/cosmos.git/dbt"

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"

# DEFAULT_DBT_ROOT_PATH = "/opt/airflow/git/cosmos.git/dbt"
# /opt/airflow/git/DFS.git/dbt_project/a
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
print(DBT_ROOT_PATH)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={"schema": "ins_bkp"},
    ),
)


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def basic_cosmos_task_group() -> None:
    """
    The simplest example of using Cosmos to render a dbt project as a TaskGroup.
    """
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    jaffle_shop = DbtTaskGroup(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "dbt_pilot",
            # manifest_path= DBT_ROOT_PATH / "jaffle_shop/target/manifest.json"
        ),
        profile_config=profile_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
        )
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> jaffle_shop >> post_dbt


basic_cosmos_task_group()
