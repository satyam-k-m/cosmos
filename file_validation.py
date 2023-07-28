import pendulum
from azure.storage.blob import BlobServiceClient
import pendulum
from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun  import TriggerDagRunOperator
import file_processor as file_processor
# from file_processor import *
# import file_processor


CONTAINER_NAME = "dfsfiles"
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=skmstorage01;AccountKey=82AORD4SS64vDMh+mSiZ67k/jX5fJX2CMaoKq8OuDgl+L/omw78P7bpzCVRFAAQgVWrB9q+piTuW+AStrhUOJQ==;EndpointSuffix=core.windows.net"
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(CONNECTION_STRING)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(CONTAINER_NAME)
CURRENT_DATE = pendulum.now("UTC").format("YYYYMMDD")


# def set_dag_vars():
#     Variable.set("start_date_var", pendulum.now())

def process_file(CONTAINER_CLIENT, CURRENT_DATE,dag_run=None, ti=None, **context):
    #print(f"dag context:", context["dag_run"].conf)
    trigger_file = dag_run.conf.get("trigger_file_name")
    print(trigger_file)
    #trigger_file = context["dag_run"].conf["trigger_file_name"]
    file_processor = file_processor.FileProcessor(trigger_file, CONTAINER_CLIENT, CURRENT_DATE)
    file_processor.process_files()


def dag_generator(source, division):
    dag_id = f"file_validation_{source.upper()}_{division.upper()}"
    with DAG(
    dag_id=dag_id, # The name that shows up in the UI
    start_date=pendulum.now(), # Start date of the DAG
    catchup=False,
    ) as dag:

        file_validate = PythonOperator(
            task_id=f"validate_files_{source}_{division}",
            provide_context=True,
            python_callable=process_file,
            op_args={CONTAINER_CLIENT:"CONTAINER_CLIENT", CURRENT_DATE: "CURRENT_DATE"}
        )


        trigger_dbt_dag = TriggerDagRunOperator(
        task_id = f'trigger_dbt_{source.upper()}_{division.upper()}',
        trigger_dag_id = f'dbt_process_{source.upper()}_{division.upper()}'
        
        )

        trigger_validation_dag = TriggerDagRunOperator(
            task_id = "wait_for_trigger_file_again",
            trigger_dag_id= "file_sensor"
            
        )
    
        file_validate >> trigger_dbt_dag >> trigger_validation_dag

        globals()[dag_id] = dag

if __name__ == "__main__":
    dag.cli()

