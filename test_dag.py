import os
import json
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


def check_env(ti=None):
	home_dir = os.environ["HOME"]
	print("home_dir :", home_dir)

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent
with DAG(dag_id="check_dbt", start_date=pendulum.now()) as dag:
      
        

      list_libs = BashOperator(
         task_id = "root_path",
         bash_command = f"echo {DEFAULT_DBT_ROOT_PATH}"

      )

      list_dirs = BashOperator(
           task_id = "list_folders",
           bash_command= f"ls  {DEFAULT_DBT_ROOT_PATH}"
      )



   

      list_libs >> list_dirs

    
     
      


if __name__ == "__main__":
  dag.cli()