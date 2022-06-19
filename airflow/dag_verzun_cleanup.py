"""This DAG is for cleaning the project directory on S3 bucket

Currently meant to be used for debugging but can one day be modified
to free up unused space. Caution: deletes the directory completely.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from verzun_functions import (_start_message, _cleanup, 
                    _success_message, _failed_message)

            
with DAG(
    dag_id="dag_verzun_cleanup",
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
    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=_cleanup,
        op_args=["{{var.value.snowpipe_files}}"]
    )
    success = PythonOperator(
        task_id = "success",
        python_callable = _success_message
        )
    failed = PythonOperator(
        task_id = "failed",
        python_callable = _failed_message,
        trigger_rule = TriggerRule.ONE_FAILED
        )
    
    start >> cleanup >> [success, failed]
    