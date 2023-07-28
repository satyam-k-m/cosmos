import os
import json
import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from airflow.operators.python import PythonOperator
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
"""
An example DAG that uses Cosmos to render a dbt project as a TaskGroup.
"""
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig,LoadMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
print(DBT_ROOT_PATH)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
    ),
)

DATA_SCHEMA = 'INS_BKP'
DB = "insight_dev"

# BLOB_NAME = "trigger.txt"
# AZURE_CONTAINER_NAME = "input"
table_list = [ "ext_chrg_info","ext_divison", "ext_lcl_currency", "ext_pos_shop", "ext_pos_trmnl", "ext_pos_dscnt", "ext_pos_tndr", "ext_pos_tx", "ext_pos_tx_dct", "ext_pos_tx_ln", "ext_rfnd_tx_rf", "ext_tndr_type", "ext_tx_type"]
SQL_REFRESH_STATEMENT = "ALTER EXTERNAL TABLE %(DB)s.%(AUDIT_SCHEMA)s.%(table_name)s REFRESH"
SQL_LIST = [ SQL_REFRESH_STATEMENT % {"table_name": table_name, 'DB':DB, "AUDIT_SCHEMA":DATA_SCHEMA}  for table_name in table_list ]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)

SNOWFLAKE_CONN_ID = "snowflake_default"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 7, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': None,
}

def get_execution_date(execution_date=None, ti=None, **context):
    run_ts= execution_date.strftime('%Y%m%d%H%M%S')
    print(run_ts)
    return run_ts


def dbt_dag_generator(source, division):
    dag_id = f'dbt_process_{source}_{division}'


    with DAG(
        dag_id=dag_id, # The name that shows up in the UI
        # Start date of the DAG
        catchup=False,
        default_args=default_args
        ) as dag:

        """
        The simplest example of using Cosmos to render a dbt project as a TaskGroup.
        """
        get_execution_date_task = PythonOperator(
            task_id = 'get_execution_date',
            python_callable=get_execution_date,
            provide_context=True

        )

        refresh_ext_tables = SnowflakeOperator(
            task_id = 'refresh_ext_tables',
            sql = SQL_MULTIPLE_STMTS
        )
        
        execution_date = "{{task_instance.xcom_pull(key='return_value', task_ids='get_execution_date')}}"
        dbt_dag = DbtTaskGroup(
            project_config=ProjectConfig(
                DBT_ROOT_PATH / "dbt_pilot",
                manifest_path= DBT_ROOT_PATH / "dbt_pilot/target/manifest.json"
            ),
            profile_config=profile_config,
            operator_args = {"vars": f'{"division":{division}, "run_id":{execution_date}}'}
        )


        get_execution_date_task >>  refresh_ext_tables >> dbt_dag


    # basic_cosmos_task_group()
    globals()[dag_id] = dag_id


# if __name__ == "__main__":
#   dag.cli()
