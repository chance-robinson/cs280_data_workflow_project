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
    my_bearer_token = "AAAAAAAAAAAAAAAAAAAAAHrdlQEAAAAAu2vIvvakLLbGqgsBXAcjwyK6XQo%3Db3PuxzKm28q0lZQUZ6N55qocL7t2YQ4no6FEET9nfURgIb2YkC"
    header_token = {"Authorization": f"Bearer {my_bearer_token}"}
    users = json.loads(ti.xcom_pull(key="users", task_ids="load_data_task"))
    # tweets = json.loads(ti.xcom_pull(key="tweets", task_ids="load_data_task"))
    user_requests = [requests.get(f"https://api.twitter.com/2/users/{id[0]}?user.fields=public_metrics,created_at", headers=header_token).json() for id in users]
    user_latest_tweet = [requests.get(f"https://api.twitter.com/2/users/{id[0]}/tweets?max_results=5", headers=header_token).json() for id in users]
    print(user_latest_tweet[0]["data"])
    user_latest_tweet = [[id[0], user_latest_tweet[idx]["data"][0]["id"]] for idx,id in enumerate(users)]
    user_latest_updated = [requests.get(f"https://api.twitter.com/2/tweets/{id[1]}?tweet.fields=public_metrics,created_at,author_id", headers=header_token).json() for id in user_latest_tweet]
    ti.xcom_push("user_info", json.dumps(user_requests))
    ti.xcom_push("latest_tweets_info", json.dumps(user_latest_updated))

def transform_data(ti: TaskInstance, **kwargs):
    user_info = json.loads(ti.xcom_pull(key="user_info", task_ids="call_api_task"))
    tweet_info = json.loads(ti.xcom_pull(key="latest_tweets_info", task_ids="call_api_task"))
    print(user_info)
    print(tweet_info)
    
def write_data():
    return 0

with DAG(
    dag_id="data_warehouse",
    schedule_interval="*/1 * * * *", # schedule_interval="*/2 * * * *", "0 9 * * *",
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