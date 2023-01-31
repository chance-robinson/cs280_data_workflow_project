from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.models import TaskInstance
import requests
import pandas as pd

def get_auth_header():
    my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN", deserialize_json=True)
    return {"Authorization": f"Bearer {my_bearer_token}"}

def get_twitter_api(ti: TaskInstance, **kwargs):
    user_ids = Variable.get("TWITTER_USER_IDS", deserialize_json=True)
    tweet_ids = Variable.get("TWITTER_TWEET_IDS", deserialize_json=True)
    user_requests = [requests.get(f"https://api.twitter.com/2/users/{id}?user.fields=public_metrics,profile_image_url,username,id,description", headers=get_auth_header()) for id in user_ids]
    tweet_requests = [requests.get(f"https://api.twitter.com/2/tweets/{id}?tweet.fields=author_id,text,public_metrics", headers=get_auth_header()) for id in tweet_ids]
    ti.xcom_push("user_requests", user_requests)
    ti.xcom_push("tweet_requests", tweet_requests)
    logging.info(user_requests,tweet_requests)

def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
    user_requests = pd.DataFrame(data=ti.xcom_pull(key="user_requests", task_ids="get_twitter_api_data_task"))
    tweet_requests = pd.DataFrame(data=ti.xcom_pull(key="tweet_requests", task_ids="get_twitter_api_data_task"))


with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    get_twitter_api_data_task = PythonOperator(
        task_id="get_twitter_api_data_task", 
        python_callable=get_twitter_api,
        provide_context=True
    )
    transform_twitter_api_data_task = PythonOperator(
        task_id="transform_twitter_api_data_task", 
        python_callable=transform_twitter_api_data_func,
        provide_context=True
    )

get_twitter_api_data_task >> transform_twitter_api_data_task