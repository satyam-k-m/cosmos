from airflow.decorators import dag
from cosmos.providers.dbt.task_group import DbtTaskGroup
from cosmos.providers.dbt.dag import DbtDag
from pendulum import datetime

CONNECTION_ID = "snowlake_default"
DB_NAME = "DFS_POC_DB"
SCHEMA_NAME = "P_DATA"
DBT_PROJECT_NAME = "developement"
# the path where Cosmos will find the dbt executable

DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"

DBT_ROOT_PATH = "/opt/airflow/git/cosmos.git/dbt"


example_local = DbtDag(
    dbt_project_name=DBT_PROJECT_NAME,
    conn_id=CONNECTION_ID,
    dbt_args={"schema": "P_DATA"},
    execution_mode="local",
    operator_args={
        "project_dir": DBT_ROOT_PATH,
       
    },
    # normal dag parameters
    start_date=datetime(2023, 7, 25),
    catchup=False,
    dag_id="dbt_task_cosmos",
)
