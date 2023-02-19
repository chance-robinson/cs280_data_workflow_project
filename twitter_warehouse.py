from airflow import DAG
import logging
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models import TaskInstance
import requests
from models.config import Session #You would import this from your config file
from models.users import User
from models.tweet import Tweet
import json

def load_data(ti: TaskInstance, **kwargs):   
    session = Session()
    users = session.query(User.user_id).all() 
    tweets = session.query(Tweet.tweet_id).all() 
    session.close()
    ti.xcom_push("users", json.dumps(users))
    ti.xcom_push("tweets", json.dumps(tweets))

def call_api(ti: TaskInstance, **kwargs):
    my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    header_token = {"Authorization": f"Bearer {my_bearer_token}"}
    users = data=ti.xcom_pull(key="users", task_ids="load_data_task")
    print(users)
    print(" ")
    tweets = data=ti.xcom_pull(key="tweets", task_ids="load_data_task")
    user_requests = [requests.get(f"https://api.twitter.com/2/users/{id}?user.fields=public_metrics,profile_image_url,username,id,description", headers=header_token).json() for id in users]
    tweet_requests = [requests.get(f"https://api.twitter.com/2/tweets/{id}?tweet.fields=author_id,text,public_metrics", headers=header_token).json() for id in tweets]
    print(user_requests)

def transform_data():
    return 0
    
def write_data():
    return 0

with DAG(
    dag_id="data_warehouse",
    schedule_interval="*/2 * * * *", # schedule_interval="*/2 * * * *", "0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"), # start_date=pendulum.datetime(2023, 1, 26, tz="US/Pacific"),
    catchup=False,
) as dag:
    load_data_task = PythonOperator(
        task_id="load_data_task", 
        python_callable=load_data,
        provide_context=True
    )
    call_api_task = PythonOperator(
        task_id="call_api_task", 
        python_callable=call_api,
        provide_context=True
    )
    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data,
        provide_context=True
    )
    write_data_task = PythonOperator(
        task_id="write_data_task",
        python_callable=write_data,
        provide_context=True
    )

load_data_task >> call_api_task >> transform_data_task >> write_data_task