
import pendulum
from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.trigger_dagrun  import TriggerDagRunOperator
from datetime import timedelta

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


with DAG(
  dag_id="trigger_file_validation_dag", # The name that shows up in the UI
  start_date=pendulum.datetime(2023, 7, 5), # Start date of the DAG
  catchup=False,
) as dag:

    trigger_file_list = Variable.get("tggr_file_list", 
                            default_var=["sample/mac/tggr_file"],
                            deserialize_json=True
                            )

    
    print(trigger_file_list)
    for tggr_file in trigger_file_list:
        source = tggr_file.split('/')[0]
        division = tggr_file.split('/')[1]

        task = TriggerDagRunOperator(
            task_id=f"trigger_file_validation_{source.upper()}_{division.upper()}",
            trigger_dag_id = f"file_validation_{source.upper()}_{division.upper()}",
            conf = {"trigger_file_name":tggr_file},
            dag=dag
        )

if __name__ == "__main__":
  dag.cli()

