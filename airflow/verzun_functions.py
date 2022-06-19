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
FIELD_LIST = ["types", "stats", "moves", "id", "name", "names", "types", "pokemon_species", "species"]

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
    This version just returns the full response as a json object.
    """
    try:
        result = requests.get(url).json()
    # don't forget to add exception handling later!
    except Exception as e:
        return "Something wrong"
   
    return result


@sleepy(0.2)
def _fetch_api_threaded(url: str, result_queue) -> None:
    """
    Gets a json from a specified URL using python requests library.
    Sleeps for 0.2 seconds before each call to help PokeAPI creators stay within budget.
    This version puts filters data and puts in a Queue object for convenient threading.
    """
    try:
        result = requests.get(url).json()
    # don't forget to add exception handling later!
    except Exception as e:
        return "Something wrong"
   
    result = {key: value for key, value in result.items() if key in FIELD_LIST}
    result_queue.put(str(result))


def _list_resources(url: str, **context) -> None:
    """
    Takes endpoint name and lists all available recources for this endpoint.
    Results are transferred to later tasks with  xcom.
    """
    # i haven't figured out how to et rid of the limit whatsoever, so it is 100000 for now 
    resource_list = list(map(lambda x: x["url"], _fetch_api(f"{url}?limit=100000")["results"]))
    context.get("task_instance").xcom_push(key="resource_list", value=resource_list)


def _load_from_resources(url: str, key_template: str, thread_number=2, **context) -> None:
    """
    Loads all data from given resources to a json file.
    If the number of resources exceeds 100 each 100 resources will be loaded to a separate partition.
    Uses a customizable number of threads(default is 2)
    """
    endpoint = url.split("/")[-2]
    resource_list = context.get("task_instance").xcom_pull(task_ids=f"find_{endpoint}", key="resource_list")

    #queue to dump results from all the threads
    result_queue = Queue()

    for number in range(0, len(resource_list), thread_number):
        threads = []
        for i in range(thread_number):
            #skip if number of remaining resources is less than number of threads
            if i > len(resource_list):
                continue

            threads.append(threading.Thread(target=_fetch_api_threaded, args=(resource_list[number+i],result_queue,)))
            threads[i].start()

        for thr in threads:
            thr.join()

    #loading result to S3
    json_string = ",".join(list(result_queue.queue))
    key = f"{key_template}{PROJECT_NAME}/{endpoint}.json"
    _load_string_on_s3(json_string, key)


def _check_generation(url:str, key_template: str, **context) -> None:
    """
    Gets the generation list from previous task. 
    Keeps records of changes to the number of generations.
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
    resource_list = context.get("task_instance").xcom_pull(task_ids=f"find_{endpoint}", key="resource_list")
    current_gen_quantity = len(resource_list)

    #find the last log (if exists)
    s3hook = S3Hook()
    key = f"{key_template}{PROJECT_NAME}/logging.log"
    logging_data = s3hook.read_key(key).split("\n")[-1]

    if context.get('dag_run').external_trigger:
        message = "Manual dag run. "
    else:
        message = "Scheduled dag run. "

    #log the changes, info if nothing changed, warning if the number changed or this is the first log
    if not logging_data:
        message += f"First check. Current number of generations is {current_gen_quantity}"
        logger.warning(message)
    else:
        previous = int(logging_data[-1].split())
        if previous == current_gen_quantity:
            message += f"Nothing changed since previous checkup. \
                There number of generations is still {current_gen_quantity}"
            logger.info(message)
        elif previous > current_gen_quantity:
            message += f"Number of generations dropped \
                from {previous} to {current_gen_quantity}"
            logger.warning(message)
        elif previous < current_gen_quantity:
            message += f"Number of generations increased \
                from {previous} to {current_gen_quantity}"
            logger.warning(message)
            
    _load_string_on_s3(log_stream.getvalue())