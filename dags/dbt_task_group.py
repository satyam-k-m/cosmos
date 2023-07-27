


CONNECTION_ID = "snowlake_default"
DB_NAME = "DFS_POC_DB"
SCHEMA_NAME = "P_DATA"
DBT_PROJECT_NAME = "developement"
# the path where Cosmos will find the dbt executable

DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"
# The path to your dbt root directory
# HOME = os.environ["HOME"] # retrieve the location of your home folder
# DBT_ROOT_PATH = os.path.join(HOME,  "DFS/dbt_project/dbt/dbt/") # path to your dbt project
DBT_ROOT_PATH = "/opt/airflow/git/cosmos.git/dbt"


from pendulum import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos.providers.dbt.task_group import DbtTaskGroup


with DAG(
    dag_id="dbt_task_group",
    start_date=datetime(2022, 11, 27),
    schedule="@daily",
):
    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg = DbtTaskGroup(
        dbt_project_name="development",
        conn_id="snowflake_default",
        dbt_args={
            "dbt_executable_path": DBT_EXECUTABLE_PATH,
            "schema": "public",
        },
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> dbt_tg >> e2
