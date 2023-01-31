import requests
import json

def get_auth_header():
  my_bearer_token = "AAAAAAAAAAAAAAAAAAAAAHrdlQEAAAAAu2vIvvakLLbGqgsBXAcjwyK6XQo%3Db3PuxzKm28q0lZQUZ6N55qocL7t2YQ4no6FEET9nfURgIb2YkC"
  return {"Authorization": f"Bearer {my_bearer_token}"}

user_id = "44196397"
api_url = f"https://api.twitter.com/2/users/{user_id}"
request = requests.get(api_url, headers=get_auth_header())
# print(request.json())


# def get_auth_header():
#     my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN", deserialize_json=True)
#     return {"Authorization": f"Bearer {my_bearer_token}"}

def get_twitter_api():
    user_ids = [44196397,62513246,11348282,12044602,2557521]
    tweet_ids = [1617886106921611270,1617975448641892352,1616850613697921025,1612900428538351616,1620198552633843713]
    user_requests = [requests.get(f"https://api.twitter.com/2/users/{id}?user.fields=public_metrics,profile_image_url,username,id,description", headers=get_auth_header()).json() for id in user_ids]
    tweet_requests = []
    for id in tweet_ids:
        tweet_requests.append(requests.get(f"https://api.twitter.com/2/tweets/{id}?tweet.fields=author_id,text,public_metrics", headers=get_auth_header()).json())
    print(user_requests)

    
get_twitter_api()
