import pendulum
from azure.storage.blob import BlobServiceClient
import csv
from airflow.sensors.base import BaseSensorOperator
from re import match
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import pendulum
from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun  import TriggerDagRunOperator
from datetime import timedelta


CONTAINER_NAME = "dfsfiles"
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=skmstorage01;AccountKey=82AORD4SS64vDMh+mSiZ67k/jX5fJX2CMaoKq8OuDgl+L/omw78P7bpzCVRFAAQgVWrB9q+piTuW+AStrhUOJQ==;EndpointSuffix=core.windows.net"
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(CONNECTION_STRING)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(CONTAINER_NAME)
CURRENT_DATE = pendulum.now("UTC").format("YYYYMMDD")


# def set_dag_vars():
#     Variable.set("start_date_var", pendulum.now())

class CustomHook(WasbHook):
    def get_blob_list_recursive(self, container_name, endswith: str = ""):
        container = self._get_container_client(container_name)
        blob_list = []
        blobs = container.list_blobs()
        for blob in blobs:
            if blob.name.endswith(endswith):
                blob_list.append(blob.name)

        return blob_list
        
class CustomWasbSensor(BaseSensorOperator):
    def poke(self, context):
        prefix = "some/constant/prefix/"
        pattern = r".*TRGGR.[a-zA-Z]+$"

        ti = context['task_instance']
        provide_context=True,
        hook = CustomHook('azure_blob')
        files = hook.get_blob_list_recursive(CONTAINER_NAME) 
        matched_tggr_file = list(filter(lambda file: match(pattern, file), files))
        print(matched_tggr_file)
        
        if len(matched_tggr_file) > 0:
            Variable.set(key="tggr_file_list",value=matched_tggr_file, serialize_json=True)
            ti.xcom_push(key='matched_tggr_file', value=matched_tggr_file)
            return matched_tggr_file
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
  dag_id="file_sensor", # The name that shows up in the UI
  default_args=default_args,
  catchup=False,
) as dag:
    
    wait_for_blob = CustomWasbSensor(
        task_id="waiting_for_trigger_file",
    )

    task = TriggerDagRunOperator(
            task_id=f"trigger_file_validation",
            trigger_dag_id="trigger_file_validation_dag",
            conf = {"trigger_file_name":"{{task_instance.xcom_pull(key='matched_tggr_file', task_ids='waiting_for_trigger_file')}}"}
        )


    wait_for_blob >> task

if __name__ == "__main__":
  dag.cli()

