import requests
import logging
from functools import wraps
from time import sleep
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

from verzun_functions import (_start_message, _list_resources,
                    _check_generation, _success_message, _failed_message)

            
with DAG(
    dag_id="dag_verzun_check_generations",
    schedule_interval="@daily",
    start_date=days_ago(2),
    catchup=False,
    max_active_runs=1,
    tags=["de_school", "final_project"]
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=_start_message
    )
    find_generation = PythonOperator(
        task_id="find_generation",
        python_callable=_list_resources,
        op_args=["https://pokeapi.co/api/v2/generation/"]
    )
    check_generation = PythonOperator(
        task_id="check_generation",
        python_callable=_check_generation,
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
    
    start >> find_generation >> check_generation >> [success, failed]