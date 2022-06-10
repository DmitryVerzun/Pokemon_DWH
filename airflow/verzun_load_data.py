import requests
import datetime
import json
from functools import wraps
from time import sleep
from io import StringIO
from typing import Dict, Any

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from functions import *

with DAG(
    dag_id="verzun_load_data",
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    max_active_runs=1,
    tags=["de_school", "final_project"]
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=_start_message
    )
    find_pokemon = PythonOperator(
        task_id="find_pokemon",
        python_callable=_list_resources,
        op_args=["https://pokeapi.co/api/v2/pokemon/"]
    )
    load_pokemon = PythonOperator(
        task_id="load_pokemon",
        python_callable=_load_from_resources,
        op_args=["https://pokeapi.co/api/v2/pokemon/", "{{var.value.snowpipe_files}}"]
    )
    find_generation = PythonOperator(
        task_id="find_generation",
        python_callable=_list_resources,
        op_args=["https://pokeapi.co/api/v2/generation/"]
    )
    load_generation = PythonOperator(
        task_id="load_generation",
        python_callable=_load_from_resources,
        op_args=["https://pokeapi.co/api/v2/generation/", "{{var.value.snowpipe_files}}"]
    )
    success = PythonOperator(
        task_id = 'success',
        python_callable = _success_message
        )
    failed = PythonOperator(
        task_id = 'failed',
        python_callable = _failed_message,
        trigger_rule = TriggerRule.ONE_FAILED
        )

    start >> find_pokemon >> load_pokemon >> [success, failed]
    start >> find_generation >> load_generation >> [success, failed]
