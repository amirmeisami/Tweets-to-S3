import tweepy
import psycopg2
import pandas.io.sql as psql
from unidecode import unidecode
import pandas as pd
import datetime
from datetime import date, timedelta
import pytz
import tinys3

db = psycopg2.connect(host="***",database="***",port="***",user="***",password="****")
sqlq = "select hashtag from twitter_hashtags_to_pull where whitelist=1;"
resultdata = psql.read_sql(sqlq, db)
db.close()
dfList = resultdata.hashtag.tolist()
my_clean_list = [unidecode(x.decode('utf8')) for x in dfList]

consumer_key = '***'
consumer_secret = '***'
access_token = '***'
access_token_secret = '***'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

def toDataFrame(tweets):

    DataSet = pd.DataFrame()

    DataSet['Hashtag'] = [tag for i in xrange(0,len(tweets))]
    DataSet['tweetCreated'] = [tweet.created_at for tweet in tweets]
    DataSet['tweetSource'] = [tweet.source for tweet in tweets]
    DataSet['userID'] = [tweet.user.id for tweet in tweets]
    DataSet['userScreen'] = [tweet.user.screen_name for tweet in tweets]
    DataSet['userName'] = [tweet.user.name for tweet in tweets]
    DataSet['userCreateDt'] = [tweet.user.created_at for tweet in tweets]
    DataSet['userFollowerCt'] = [tweet.user.followers_count for tweet in tweets]
    DataSet['userStatusesCt'] = [tweet.user.statuses_count for tweet in tweets]
    DataSet['userFriendsCt'] = [tweet.user.friends_count for tweet in tweets]
    DataSet['tweetRetweetCt'] = [tweet.retweet_count for tweet in tweets]
    DataSet['userLocation'] = [tweet.user.location for tweet in tweets]
    DataSet['userTimezone'] = [tweet.user.time_zone for tweet in tweets]
    DataSet['tweetLan']=[tweet.user.lang for tweet in tweets]
    DataSet['tweetText'] = [tweet.text for tweet in tweets]

    return DataSet

eastern_tz = pytz.timezone('US/Pacific-New')
df = pd.DataFrame()
DataSet={}
for tag in my_clean_list:
    results=[]
    for tweet in tweepy.Cursor(api.search,q=tag,since=str(datetime.datetime.now(eastern_tz).date()-timedelta(days=1)),
                               until=str(datetime.datetime.now(eastern_tz).date()),count=100).items():
        results.append(tweet)

    DataSet['df_%s' % tag]= toDataFrame(results)
   
tweet_data = pd.concat([DataSet['df_%s' % tag] for tag in my_clean_list])
tweet_data.to_csv('tweet_data_%s.csv' %str(datetime.datetime.now(eastern_tz).date()-timedelta(days=1)),encoding='utf-8',index=False)

conn = tinys3.Connection('***','***',endpoint='s3-us-west-2.amazonaws.com')
f = open('tweet_data_%s.csv' %str(datetime.datetime.now(eastern_tz).date()-timedelta(days=1)),'rb')
conn.upload('tweet_data_%s.csv' %str(datetime.datetime.now(eastern_tz).date()-timedelta(days=1)),f,'~/twitter')
