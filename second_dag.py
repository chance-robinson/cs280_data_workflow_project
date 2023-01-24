from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def first_task_function():
    log.info("Second Dag")
    name = "Enter dag \# here"
    log.info(f"Dag \#{dag}")
    return

with DAG(
    dag_id="my_second_cs280_dag",
    schedule_interval="0 5 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    first_task = PythonOperator(task_id="first_task", python_callable=first_task_function)
    end_task = DummyOperator(task_id="end_task")

start_task >> first_task >> end_task
