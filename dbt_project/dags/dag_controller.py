import os
import sys
from pathlib import Path
from airflow.models import Variable
import json
import dbt_project.dags.dbt_dag as dbt_dag
import dbt_project.dags.file_validation as file_validation


config_file_path = Path(__file__) / "config.json"


with open(config_file_path, 'r') as json_file:
    # Load the JSON data
    config = json.load(json_file)
    config_dags = config["dags_mapping"]

print(config)

for source, division in config_dags.items():
    if "div" in division:
        divisions = division["div"]
        for div in divisions:
            dbt_dag.dbt_dag_generator(source, div)
            file_validation.dag_generator(source, div)