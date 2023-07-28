

import csv
import pendulum
from airflow.models import DagRun

class FileProcessor():
    def __init__(self, trigger_file, CONTAINER_CLIENT, CURRENT_DATE):
        self.trigger_file = trigger_file
        self.CONTAINER_CLIENT = CONTAINER_CLIENT
        self.source = trigger_file.split("/")[0]
        self.division = trigger_file.split("/")[1]
        self.root_file_path = f"{self.source}/{self.division}"
        self.external_file_path = f"{self.source}"
        self.CURRENT_DATE = CURRENT_DATE

    def get_control_files(self):
        control_file_pattern = f"{self.division.upper()}.CF"
        control_file_list = [blob.name 
                            for blob in self.CONTAINER_CLIENT.list_blobs() 
                            if blob.name.startswith(self.root_file_path) and blob.name.endswith(control_file_pattern)]


        return control_file_list
    
    def check_status(self, date_str):
        dag_id = f'dbt_process_{self.source}_{self.division}'
        try:
            date_format = 'DD-MM-YYYY HH:mm:ss'
            extracted_date = pendulum.from_format(date_str, date_format).date()
            task_instances = DagRun.find(
                    dag_id=dag_id, state='success')
            for task_instance in task_instances:
                if task_instance.execution_date.date() == extracted_date:
                    return True

            return False

        except ValueError:
            print(f"Invalid date format: {date_str} doesn't match with {date_format}")


    def check_dag_run_status(self, CONTAINER_CLIENT,control_files_list):
        already_processed = False
        for file in control_files_list:
            print(file)
            blob_client = CONTAINER_CLIENT.get_blob_client(file)
            blob_data = blob_client.download_blob().readall().decode('utf-8')
            print(blob_data)
            reader = csv.reader(blob_data.splitlines(), delimiter="|")
            for row in reader:        
                date_str = row[2]
                file_name = row[1]
                if self.check_status(date_str):
                    
                    already_processed = True
                else:
                    print(f"file not processed before")


    def parse_control_files(self, control_file_list):
        already_processed = False
        data_files_list = []
        for file in control_file_list:
            print(file)
            blob_client = self.CONTAINER_CLIENT.get_blob_client(file)
            blob_data = blob_client.download_blob().readall().decode('utf-8')
            print(blob_data)
            reader = csv.reader(blob_data.splitlines(), delimiter="|")
            for row in reader:
                data_file = row[1]
                date_str = row[2]
                if self.check_status(date_str):
                    print(f"Task succeeded for {file} on {date_str}")
                    already_processed = True
                else:
                    print("file {data_file}not processed before")
                    data_files_dict={'file_name': f"{row[1]}", 'expected_count': row[3]}
                    data_files_list.append(data_files_dict)
        if not already_processed:
            self.validate_data_files(data_files_list, control_file_list, already_processed)

    def move_files_to_folder(self, file_name, destination_folder):
        if destination_folder == 'external':
            root_path = self.external_file_path
        else:
            root_path = self.root_file_path
        source_blob_client = self.CONTAINER_CLIENT.get_blob_client(f"{self.root_file_path}/{file_name}")
        if source_blob_client.exists():
            file = f"{file_name}_{self.CURRENT_DATE}"
            destination_blob_client = self.CONTAINER_CLIENT.get_blob_client(f"{root_path}/{destination_folder}/{file}")
            destination_blob_client.start_copy_from_url(source_blob_client.url)
            source_blob_client.delete_blob()
    
    def validate_data_files(self, data_files_list, control_files_list, already_processed):
        all_files_present= True
        all_matched_records = True
        # Iterate through the data files
        for data_file in data_files_list:
            file_name = data_file['file_name']
            expected_count = data_file['expected_count']
            print(f"Processing {file_name} has {expected_count} records")
            print(type(expected_count))
            # Get a reference to the data file
            blob_client = self.CONTAINER_CLIENT.get_blob_client(f"{self.root_file_path}/{file_name}")
            
            # Check if the data file exists in the container
            if blob_client.exists():
                # Download the data file content
                file_data = blob_client.download_blob().readall().decode('utf-8')
                
                # Count the number of records in the data file
                actual_count = int(len(file_data.split('\n'))) - 2 
                print(type(actual_count))
                # Compare the actual count with the expected count
                if int(actual_count) == int(expected_count):
                    print(f"Data file '{file_name}' is present. Expected record count:{expected_count} matching with Actual Count: {actual_count}")

                else:
                    all_matched_records = False
                    print(f"Data file '{file_name}' is present. Expected record count of {expected_count} not matching Actual Count: {actual_count}")
            else:
                all_files_present = False
                print(f"Data file '{file_name}' does not exist in the container.")


        # Move files to appropriate folders
        for file_dict in data_files_list:
            file_name =file_dict["file_name"]

            if all_files_present and all_matched_records and not already_processed:
                destination_folder = 'external'
            else:
                destination_folder = "failed"

            self.move_files_to_folder(file_name, destination_folder)

        for ctrl_file_name in control_files_list:
            print("Moving CONTROLS file to archive folder")
            ctrl_file_name = ctrl_file_name.split("/")[-1]
            destination_folder = 'archived'
            self.move_files_to_folder(ctrl_file_name, destination_folder)

        print("Moving TRIGGER file to archive folder")

        destination_folder = 'archived'
        trigger_file = self.trigger_file.split("/")[-1]
        self.move_files_to_folder(trigger_file, destination_folder)

        if all_files_present==False: 
            raise FileNotFoundError("Some file is missing...moving the files to failed folder.")
        elif all_matched_records==False:
            raise ValueError("Records does not match...moving the files to failed folder.")


    def process_files(self):
        control_files_list = self.get_control_files()
        print("LIST OF CONTROL FILES:", control_files_list)
        data_files_list = self.parse_control_files(control_files_list)
        print("LIST OF DATA FILES:", data_files_list)

