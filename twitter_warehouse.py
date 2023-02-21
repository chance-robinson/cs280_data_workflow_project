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
from models.user_timeseries import User_Timeseries
from models.tweet_timeseries import Tweet_Timeseries
import json
import pandas as pd
from google.cloud import storage
from gcsfs import GCSFileSystem
import csv
from datetime import datetime


def flatten_list_of_dicts(lst):
    results = []
    for item in lst:
        result = {}
        flatten_dict("", item, result)
        results.append(result)
    return results

def flatten_dict(prefix, d, result):
    for key, value in d.items():
        if isinstance(value, dict):
            flatten_dict(f"{prefix}{key}.", value, result)
        else:
            result[f"{prefix}{key}"] = value

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
    user_latest_tweet = [user_latest_tweet[idx]["data"][0]['id'] for idx,id in enumerate(users)]
    user_latest_updated = [requests.get(f"https://api.twitter.com/2/tweets/{id}?tweet.fields=public_metrics,created_at,author_id", headers=header_token).json() for id in user_latest_tweet]
    ti.xcom_push("user_requests", json.dumps(user_requests))
    ti.xcom_push("user_latest_updated", json.dumps(user_latest_updated))

def transform_data(ti: TaskInstance, **kwargs):
    user_info = json.loads(ti.xcom_pull(key="user_requests", task_ids="call_api_task"))
    tweet_info = json.loads(ti.xcom_pull(key="user_latest_updated", task_ids="call_api_task"))
    user_dict = flatten_list_of_dicts(user_info)
    tweet_dict = flatten_list_of_dicts(tweet_info)
    user_df = pd.DataFrame(user_dict)
    tweet_df = pd.DataFrame(tweet_dict)

    client = storage.Client()
    bucket = client.get_bucket("c-r-apache-airflow-cs280")
    bucket.blob("data/users.csv").upload_from_string(user_df.to_csv(index=False), "text/csv")
    bucket.blob("data/tweets.csv").upload_from_string(tweet_df.to_csv(index=False), "text/csv")
    
def write_data():
    def header_index_vals(header, match_headers):
        values = []
        for val in match_headers:
            for idx,item in enumerate(header):
                if val == item:
                    values.append(idx)
        myDict = { k:v for (k,v) in zip(match_headers, values)} 
        return myDict

    def create_data_users(header, match_headers, val):
        session = Session()
        # update
        myDict = header_index_vals(header, match_headers)
        q = session.query(User)
        if (q.filter(User.user_id==val[myDict['data.id']])):
            q = q.filter(User.user_id==val[myDict['data.id']])
            record = q.one()
            record.username = val[myDict['data.username']]
            record.name = val[myDict['data.name']]
            record.created_at = val[myDict['data.created_at']]
        # create
        else:
            user = User(
                user_id = val[myDict['data.id']],
                username = val[myDict['data.username']],
                name = val[myDict['data.name']],
                created_at = val[myDict['data.created_at']]
            )
            session.add(user)
        session.commit()
        session.close()

    def create_data_users_timeseries(header, match_headers, val):
        session = Session()
        myDict = header_index_vals(header, match_headers)
        q = session.query(User_Timeseries)
        print(val)
        print(myDict)
        if (q.filter(User_Timeseries.user_id==val[myDict['data.id']])) and (q.all()):
            record = q.filter(User_Timeseries.user_id==val[myDict['data.id']]).first()
            record.followers_count = val[myDict['data.public_metrics.followers_count']]
            record.following_count = val[myDict['data.public_metrics.following_count']]
            record.tweet_count = val[myDict['data.public_metrics.tweet_count']]
            record.listed_count = val[myDict['data.public_metrics.listed_count']]
            record.date = datetime.now()
        # create
        else:
            user_timeseries = User_Timeseries(
                user_id = val[myDict['data.id']],
                followers_count = val[myDict['data.public_metrics.followers_count']],
                following_count = val[myDict['data.public_metrics.following_count']],
                tweet_count = val[myDict['data.public_metrics.tweet_count']],
                listed_count = val[myDict['data.public_metrics.listed_count']],
                date = datetime.now()
            )
            session.add(user_timeseries)
        session.commit()
        session.close()

    def create_data_tweets_timeseries(header, match_headers, val):
        session = Session()
        myDict = header_index_vals(header, match_headers)
        q = session.query(Tweet_Timeseries)
        if (q.filter(Tweet_Timeseries.tweet_id==val[myDict['data.id']])):
            q = q.filter(Tweet_Timeseries.tweet_id==val[myDict['data.id']])
            record = q.one()
            record.tweet_id = val[myDict['data.id']]
            record.retweet_count = val[myDict['data.public_metrics.retweet_count']]
            record.favorite_count = val[myDict['data.public_metrics.like_count']]
            record.date = datetime.now()
        # create
        else:
            tweet_timeseries = Tweet_Timeseries(
                tweet_id = val[myDict['data.id']],
                retweet_count = val[myDict['data.public_metrics.retweet_count']],
                favorite_count = val[myDict['data.public_metrics.like_count']],
                date = datetime.now()
            )
            session.add(tweet_timeseries)
        session.commit()
        session.close()

    def create_data_tweets(header, match_headers, val):
        session = Session()
        myDict = header_index_vals(header, match_headers)
        q = session.query(Tweet)
        if (q.filter(Tweet.tweet_id==val[myDict['data.id']])):
            q = q.filter(Tweet.tweet_id==val[myDict['data.id']])
            record = q.one()
            record.text = val[myDict['data.text']]
            record.created_at = val[myDict['data.created_at']]
        # create
        else:
            tweet = Tweet(
                tweet_id = val[myDict['data.id']],
                user_id = val[myDict['data.author_id']],
                text = val[myDict['data.text']],
                created_at = val[myDict['data.created_at']]
            )
            session.add(tweet)
        session.commit()
        session.close()
    
    fs = GCSFileSystem(project="Chance-Robinson-CS-280")
    with fs.open('gs://c-r-apache-airflow-cs280/data/users.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        data = [row for row in reader]
        user_headers = ['data.id','data.username','data.name','data.created_at']
        user_timeseries_headers = ['data.id','data.public_metrics.followers_count','data.public_metrics.following_count','data.public_metrics.tweet_count', 'data.public_metrics.listed_count']
        for val in data:
            create_data_users(header, user_headers, val)
            create_data_users_timeseries(header, user_timeseries_headers, val)

    # with fs.open('gs://c-r-apache-airflow-cs280/data/tweets.csv', 'r') as csvfile:
    #     reader = csv.reader(csvfile)
    #     header = next(reader)
    #     data = [row for row in reader]
    #     tweet_headers = ['data.id','data.author_id','data.text','data.created_at']
    #     tweet_timeseries_headers = ['data.id','data.public_metrics.retweet_count','data.public_metrics.like_count']
    #     for val in data:
    #         create_data_tweets(header, tweet_headers, val)
    #         create_data_tweets_timeseries(header, tweet_timeseries_headers, val)

with DAG(
    dag_id="data_warehouse",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    # load_data_task = PythonOperator(
    #     task_id="load_data_task", 
    #     python_callable=load_data,
    #     provide_context=True
    # )
    # call_api_task = PythonOperator(
    #     task_id="call_api_task", 
    #     python_callable=call_api,
    #     provide_context=True
    # )
    # transform_data_task = PythonOperator(
    #     task_id="transform_data_task",
    #     python_callable=transform_data,
    #     provide_context=True
    # )
    write_data_task = PythonOperator(
        task_id="write_data_task",
        python_callable=write_data,
        provide_context=True
    )

# load_data_task >> call_api_task >> transform_data_task >> write_data_task
write_data_task