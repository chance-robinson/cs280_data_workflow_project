from airflow import DAG
import logging
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models import TaskInstance
import requests
import pandas as pd
from google.cloud import storage
import json

def flatten_json(data_dict, matching_data, keys_to_match):
    for key, value in data_dict.items():
        if type(value) is dict:
            flatten_json(value, matching_data, keys_to_match)
        else:
            if key in keys_to_match:
                matching_data.append(f"{key}: {value}")
    return matching_data

def iterate_json_list(data_dict, keys_to_match):
    match_list = []
    for dicti in data_dict:
        matches = []
        if type(dicti) is dict:
            flatten_json(dicti, matches, keys_to_match)
        matches = {x.split(": ")[0]: x.split(": ")[1] for x in matches}
        matches = dict(sorted(matches.items()))
        match_list.append(matches)
    return pd.DataFrame(match_list)

def get_twitter_api(ti: TaskInstance, **kwargs):
    user_ids = Variable.get("TWITTER_USER_IDS", deserialize_json=True)
    tweet_ids = Variable.get("TWITTER_TWEET_IDS", deserialize_json=True)
    my_bearer_token = "AAAAAAAAAAAAAAAAAAAAAHrdlQEAAAAAu2vIvvakLLbGqgsBXAcjwyK6XQo%3Db3PuxzKm28q0lZQUZ6N55qocL7t2YQ4no6FEET9nfURgIb2YkC"
    header_token = {"Authorization": f"Bearer {my_bearer_token}"}
    user_requests = [requests.get(f"https://api.twitter.com/2/users/{id}?user.fields=public_metrics,profile_image_url,username,id,description", headers=header_token).json() for id in user_ids]
    tweet_requests = [requests.get(f"https://api.twitter.com/2/tweets/{id}?tweet.fields=author_id,text,public_metrics", headers=header_token).json() for id in tweet_ids]
    ti.xcom_push("user_requests", user_requests)
    ti.xcom_push("tweet_requests", tweet_requests)
    logging.info(user_requests)
    logging.info(tweet_requests)

def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
    client = storage.Client()
    bucket = client.get_bucket("c-r-apache-airflow-cs280")
    
    users = pd.DataFrame(data=ti.xcom_pull(key="user_requests", task_ids="get_twitter_api_data_task"))
    tweets = pd.DataFrame(data=ti.xcom_pull(key="tweet_requests", task_ids="get_twitter_api_data_task"))
    tweet_header_list = ['retweet_count', 'reply_count', 'like_count', 'quote_count', 'impression_count', 'tweet_id', 'text', 'id']
    user_header_list = ['followers_count','following_count','tweet_count','listed_count','name','username','id']
    logging.info(users)
    user_matching_data = iterate_json_list(json.loads(users), user_header_list)
    logging.info(user_matching_data)
    tweet_matching_data = iterate_json_list(json.loads(tweets), tweet_header_list)
    
    
    bucket.blob("data/user_requests.csv").upload_from_string(user_matching_data.to_csv(index=False), "text/csv")
    bucket.blob("data/tweet_requests.csv").upload_from_string(tweet_matching_data.to_csv(index=False), "text/csv")


with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="*/1 * * * *",
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