"""This file contains all the functions nessecary for the DAGs to run.

Contains the following functions:
_start_message, _list_resources,_load_from_resources,
 _success_message, _failed_message, _check_generation"""

import requests
import threading
import logging
from io import StringIO
from queue import Queue
from functools import wraps
from time import sleep
from typing import Dict, Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# project name used for naming and prefixes (my last name)
PROJECT_NAME = "Verzun"

#list of fields we are interested in
FIELD_LIST = ["types", "stats", "moves", "id", "name", "names", \
     "types", "pokemon_species", "species"]

def _start_message() -> None:
    """Prints a start message."""
    print(f"The DAG was launched!")


def _success_message() -> None:
    """Congratulates upon successful DAG completion."""
    print(f"The DAG was completed successfully! Congratulations!")
    
    
def _failed_message() -> None:
    """Prints a message if the DAG failed."""
    print(f"The DAG failed:(")


def _load_string_on_s3(data: str, key: str) -> None:
    """Loads a given string to given key on S3 bucket.
    
    Arguments:
    data: any string you want
    key: a valid S3 key
    """
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
    """Fetches data from an API and returns it as a json object.

    Arguments:
    url: adress of the target API
    
    Sleeps for 0.2 seconds before each request to limit request frequency.
    """
    result = requests.get(url).json()
   
    return result


@sleepy(0.2)
def _fetch_api_threaded(url: str, result_queue: Queue) -> None:
    """Fetches data from an API and writes it to a Queue object.

    Arguments:
    url: adress of the target API
    result_queue: the Queue object for storing result strings
    FIELD_LIST: global variable containing relevant fields,
    only those are placed in the Queue to save memory and S3 space

    Sleeps for 0.2 seconds before each request to limit request frequency.
    Filters data and puts it in a Queue object for convenient threading.
    """
    result = requests.get(url).json()
    
    result = {key: value for key, value in result.items() if key in FIELD_LIST}
    result_queue.put(str(result))


def _list_resources(url: str, **context) -> None:
    """Finds all available resources for a given endpoint.

    Arguments:
    url: adress of the target API (e.g. https://pokeapi.co/api/v2/{endpoint}/)

    Results are transferred to further tasks with xcom.
    """
    resource_list = list(map(lambda x: x["url"], \
         _fetch_api(f"{url}?limit=100000")["results"]))
    context.get("task_instance") \
        .xcom_push(key="resource_list", value=resource_list)


def _load_from_resources(url: str, key_template: str, thread_number: int=2, **context) -> None:
    """Loads data from given resources to S3 using _fetch_api_threaded.

    Arguments:
    url: adress of the target API (e.g. https://pokeapi.co/api/v2/{endpoint}/)
    key_template: adress of the target bucket given in snowpipe_files variable
    thread_number: specifies the number of threads for loading data (default is 2)
    """
    endpoint = url.split("/")[-2]
    resource_list = context.get("task_instance"). \
        xcom_pull(task_ids=f"find_{endpoint}", key="resource_list")

    #queue to dump results from all the threads simultaneously
    result_queue = Queue()

    for number in range(0, len(resource_list), thread_number):
        threads = []
        for i in range(thread_number):
            #in case there are not enough resources left
            if i > len(resource_list):
                continue

            threads.append(threading.Thread(target=_fetch_api_threaded, \
                args=(resource_list[number+i],result_queue,)))
            threads[i].start()

        for thr in threads:
            thr.join()

    #loading result to S3
    json_string = ",".join(list(result_queue.queue))
    key = f"{key_template}{PROJECT_NAME}/{endpoint}.json"
    _load_string_on_s3(json_string, key)


def _check_generation(url:str, key_template: str, **context) -> None:
    """Keeps records of changes to the number of generations.

    Arguments:
    url: adress of the target API (e.g. https://pokeapi.co/api/v2/{endpoint}/)
    key_template: adress of the target bucket given in snowpipe_files variable
    """
    #start the logger and formatter
    logger = logging.getLogger("generation_logger") 
    formatter = logging.Formatter("%(asctime)s => %(levelname)s => %(message)s")

    #add handler for S3
    log_stream = StringIO() 
    log_handler = logging.StreamHandler(log_stream)
    logger.addHandler(log_handler)
    log_handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)

    #get current list of generations and count them
    endpoint = url.split("/")[-2]
    resource_list = context.get("task_instance"). \
        xcom_pull(task_ids=f"find_{endpoint}", key="resource_list")
    current_gen_quantity = len(resource_list)

    #find the last log (if exists), else give empty string
    s3hook = S3Hook()
    key = f"{key_template}{PROJECT_NAME}/logging.log"
    if s3hook.check_for_key(key):
        logging_data = s3hook.read_key(key)
    
    else:
        logging_data = ""

    if context.get("dag_run").external_trigger:
        message = "Manual dag run. "
    else:
        message += "Scheduled dag run. "

    #info if nothing changed, warning if something did or this is the first log
    if not logging_data:
        message += f"First check. Current number \
            of generations is {current_gen_quantity}"
        logger.warning(message)
    else:
        previous = int(logging_data.split()[-1])
        if previous == current_gen_quantity:
            message += f"Nothing changed since previous checkup. \
                The number of generations is still {current_gen_quantity}"
            logger.info(message)
        elif previous > current_gen_quantity:
            message += f"Number of generations dropped \
                from {previous} to {current_gen_quantity}"
            logger.warning(message)
        elif previous < current_gen_quantity:
            message += f"Number of generations increased \
                from {previous} to {current_gen_quantity}"
            logger.warning(message)

    _load_string_on_s3(logging_data + log_stream.getvalue(), key)
    

def _cleanup(key_template: str) -> None:
    """Deletes all files from working directory

    Arguments:
    key_template: adress of the target bucket given in snowpipe_files variable
    """
    s3hook = S3Hook()
    bucket, prefix = S3Hook.parse_s3_url(key_template)
    full_prefix = f"{prefix}{PROJECT_NAME}/"
    
    keys = s3hook.list_keys(bucket, prefix=full_prefix)
    s3hook.delete_objects(bucket, keys)
