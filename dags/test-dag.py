import os
import json
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

with DAG(dag_id="bash_dir", start_date=pendulum.now()) as dag:
      
        

      list_libs = BashOperator(
         task_id = "list_pip",
         bash_command = "pip list"

      )

      list_dirs = BashOperator(
           task_id = "list_folders",
           bash_command= "ls /opt/airflow/git/cosmos.git/"
      )



   

      list_libs >> list_dirs

    
     
      


if __name__ == "__main__":
  dag.cli()