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

# project name used for naming and prefixes (my last name)
PROJECT_NAME = "Verzun"

# maximum amount of API data loaded to memory before writing to file 
# (not really sure it was a good idea)
BATCH_SIZE = 100

def _start_message() -> None:
    print(f"The DAG was launched!")


def _success_message() -> None:
    print(f"The DAG was performed successfully!")
    
    
def _failed_message() -> None:
    print(f"The DAG failed:(")


def _load_string_on_s3(data: str, key: str) -> None:
    """Loads a given string to given key on S3 bucket"""
    s3hook = S3Hook()
    s3hook.load_string(string_data=data, key=key, replace=True)


def sleepy(sleep_time):
    """Decorator. Makes a function sleep for a specified time before running."""    
    def sleepy_decorator(func):
        @wraps(func)
        def sleepy_wrapper(*args, **kwargs):
            sleep(sleep_time)
            return func(*args, **kwargs)
        return sleepy_wrapper
    return sleepy_decorator


@sleepy(0.2)
def _fetch_api(url: str) -> Dict[str, Any]:
    """
    Gets a json from a specified URL using python requests library.
    Sleeps for 0.2 seconds before each call to help PokeAPI creators stay within budget.
    Used by most other functions because fetching requirements are similar.
    """
    try:
        result = requests.get(url).json()
    # don't forget to add exception handling later!
    except Exception as e:
        return "Something wrong"
   
    return result


def _list_resources(url: str, **context) -> None:
    """
    Takes endpoint name and lists all available recources for this endpoint.
    Results are transferred to later tasks with  xcom.
    """
    # i haven't figured out how to et rid of the limit whatsoever, so it is 100000 for now 
    resource_list = list(map(lambda x: x["url"], _fetch_api(f"{url}?limit=100000")["results"]))
    context.get("task_instance").xcom_push(key="resource_list", value=resource_list)


def _load_from_resources(url: str, key_template: str, **context) -> None:
    """
    Loads all data from given resources to a json file.
    If the number of resources exceeds 100 each 100 resources will be loaded to a separate partition.
    """
    endpoint = url.split("/")[-2]
    resource_list = context.get("task_instance").xcom_pull(task_ids=f"find_{endpoint}", key="resource_list")

    #index that will be used in file name if data is too big for one file
    index = 0
    json_cache = []

    # :50 is for testing purposes only!!!!
    for number, resource in enumerate(resource_list[:50]):
        data = _fetch_api(resource)
        json_cache.append(data)

        # every n'th number i save to S3 to avoid overusing memory and empty the cache
        if (number+1) % BATCH_SIZE == 0:
            data_dict = {"all_data": json_cache}
            json_cache = []
            index += 1
            key = f"{key_template}{PROJECT_NAME}/{endpoint}_partition_{index}.json"
            _load_string_on_s3(str(data_dict), key)
            #with open(f"./{endpoint}_partition_{index}.json", 'w', encoding='utf-8') as out_file:
            #    json.dump(data_dict, out_file, ensure_ascii=False)

    #loading the last (or the only) partition
    data_dict = {"all_data": json_cache} 
    if index != 0:   
        index += 1
        key = f"{key_template}{PROJECT_NAME}/{endpoint}_partition_{index}.json"
        _load_string_on_s3(str(data_dict), key)
    else:
        key = f"{key_template}{PROJECT_NAME}/{endpoint}.json"
        _load_string_on_s3(str(data_dict), key)


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
        op_args=["{{var.value.pokemon_url}}"]
    )
    load_pokemon = PythonOperator(
        task_id="load_pokemon",
        python_callable=_load_from_resources,
        op_args=["{{var.value.pokemon_url}}", "{{var.value.output_adress}}"]
    )
    find_generation = PythonOperator(
        task_id="find_generation",
        python_callable=_list_resources,
        op_args=["{{var.value.generation_url}}"]
    )
    load_generation = PythonOperator(
        task_id="load_generation",
        python_callable=_load_from_resources,
        op_args=["{{var.value.generation_url}}", "{{var.value.output_adress}}"]
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
