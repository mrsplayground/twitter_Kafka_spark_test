#!/usr/bin/python3

import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer

# twitter setup
consumer_key = "XXXXX"
consumer_secret = "XXXXX"
access_token = "XXXXX"
access_token_secret = "XXXXX"

# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)

# Creating the API object by passing in auth information
api = tweepy.API(auth)

from datetime import datetime, timedelta

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime += timedelta(hours=6)   # the tweets are timestamped in GMT timezone, while I am in +6 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'tweets'

#code borrowed from this blog post: 
#https://dorianbg.wordpress.com/2017/11/11/ingesting-realtime-tweets-using-apache-kafka-tweepy-and-python/
def get_twitter_data():
    res = api.search("wine")
    for i in res:
        record = ''
        record += str(i.user.id_str)
        record += ';'
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record += str(i.user.followers_count)
        record += ';'
        record += str(i.user.location)
        record += ';'
        record += str(i.favorite_count)
        record += ';'
        record += str(i.retweet_count)
        record += ';'
        producer.send(topic_name, str.encode(record))

#get_twitter_data()

def periodic_work(interval):
    while True:
        get_twitter_data()
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)

periodic_work(60*1)  # get data every couple of minutes
