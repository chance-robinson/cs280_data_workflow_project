import requests
import json
import pandas as pd

def get_twitter_api():
    user_ids = [44196397,62513246,11348282,12044602,2557521]
    tweet_ids = [1617886106921611270,1617975448641892352,1616850613697921025,1612900428538351616,1620198552633843713]
    my_bearer_token = "AAAAAAAAAAAAAAAAAAAAAHrdlQEAAAAAu2vIvvakLLbGqgsBXAcjwyK6XQo%3Db3PuxzKm28q0lZQUZ6N55qocL7t2YQ4no6FEET9nfURgIb2YkC"
    header_token = {"Authorization": f"Bearer {my_bearer_token}"}
    user_requests = [requests.get(f"https://api.twitter.com/2/users/{id}?user.fields=public_metrics,profile_image_url,username,id,description", headers=header_token).json() for id in user_ids]
    tweet_requests = []
    for id in tweet_ids:
        tweet_requests.append(requests.get(f"https://api.twitter.com/2/tweets/{id}?tweet.fields=author_id,text,public_metrics", headers=header_token).json())
    user_requests = json.dumps(user_requests)
    tweet_requests = json.dumps(tweet_requests)
    return user_requests, tweet_requests

users,tweets = get_twitter_api()

# Define a list of keys to match
tweet_header_list = ['retweet_count', 'retweet_count', 'like_count', 'quote_count', 'impression_count']
user_header_list = ['followers_count','following_count','tweet_count','listed_count']

# Create an empty list to store the matching key-value pairs
tweet_match = []
user_match = []

def flatten_json2(data_dict, matching_data, keys_to_match):
    print(pd.json_normalize(data_dict).head())
    for key, value in data_dict.items():
        if type(value) is dict:
            flatten_json2(value, matching_data, keys_to_match)
        else:
            if key in keys_to_match:
                matching_data.append((key, value))
    return matching_data

# Define a function that will recursively loop through the nested dictionaries in the JSON object
def flatten_json(data_dict, matching_data, keys_to_match):
    for dicti in data_dict:
        if type(dicti) is dict:
            flatten_json2(dicti, matching_data, keys_to_match)

# Flatten the nested dictionaries in the JSON object and get matching key-value pairs
user_matching_data = flatten_json(json.loads(users), user_match, user_header_list)
# tweet_matching_data = flatten_json(json.loads(tweets), tweet_match, tweet_header_list)
# Display the matching key-value pairs
# print(user_match)
