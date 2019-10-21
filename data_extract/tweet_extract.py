# Created by Hansi at 9/6/2019
import configparser
import csv
import datetime
import time

import tweepy

ITERATOR_LENGTH = 50
API_CALL_LIMIT = 180
TIME_LIMIT = 15 * 60

configParser = configparser.RawConfigParser()
configFilePath = "../config.txt"
configParser.read(configFilePath)

CONSUMER_KEY = configParser.get('twitter-dev-configs', 'CONSUMER_KEY')
CONSUMER_SECRET = configParser.get('twitter-dev-configs', 'CONSUMER_SECRET')
OAUTH_TOKEN = configParser.get('twitter-dev-configs', 'OAUTH_TOKEN')
OAUTH_TOKEN_SECRET = configParser.get('twitter-dev-configs', 'OAUTH_TOKEN_SECRET')

FULL_DATA_SET_PATH = '../data/full_dataset/'


def get_tweet_by_id(id):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)
    tweet = api.get_status(id)

    print("Tweet_ID: " + str(id))
    print("Tweet: " + tweet.text)
    print("Timestamp: ", tweet.created_at)
    return tweet


def get_tweet_by_hashtag(hashtag, from_date, to_date):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)

    filename = hashtag.split('#')[1]

    # Open/Create a file to append data
    csvFile = open(FULL_DATA_SET_PATH + filename + '.tsv', 'a', newline='', encoding='utf-8')
    # Use csv Writer
    csvWriter = csv.writer(csvFile, delimiter='\t')

    start_time = datetime.datetime.now()
    i = 0
    for tweet in tweepy.Cursor(api.search, q=hashtag, count=100,
                               lang="en",
                               since=from_date, until=to_date).items():
        i += 1
        print(tweet.id_str, tweet.created_at, tweet.text, get_hashtags(tweet.entities.get('hashtags')))
        csvWriter.writerow([tweet.id_str, tweet.created_at, tweet.text.encode('utf-8'),
                            get_hashtags(tweet.entities.get('hashtags')).encode('utf-8'),
                            tweet.user.location.encode('utf-8')])

        # print('Sleeping for (seconds) : 1')
        time.sleep(1)

        # 450 call per 15 mins
        # current_time = datetime.datetime.now()
        # time_diff = current_time - start_time
        # if i == 450 and time_diff.seconds < TIME_LIMIT:
        #     print('Sleeping for (seconds) : ', time_diff.seconds)
        #     time.sleep(TIME_LIMIT - time_diff.seconds)


# get hash tag text from tweepy status hashtags
def get_hashtags(json_array):
    hashtag_text = ''
    for item in json_array:
        if hashtag_text == '':
            hashtag_text = item['text']
        else:
            hashtag_text = hashtag_text + "," + item['text']
    return hashtag_text


# get geographic location of this Tweet as reported by the user or client application
def get_location(coordinates):
    if coordinates is None:
        return '_na_,_na_'
    else:
        return str(coordinates[0]) + ',' + str(coordinates[1])


if __name__ == "__main__":
    # extract_by_id(2)
    # tweet = get_tweet_by_id(198808270440378371)
    # print(tweet.coordinates)
    # print(get_location(tweet.coordinates))
    # print(tweet.user)
    # print(tweet.user.location)

    # get_tweet_by_hashtag("#UCLfinal", "2019-06-01")
    # get_tweet_by_hashtag("#Barcelona", "2019-10-01")
    get_tweet_by_hashtag("#BrexitVote ", "2019-10-18", "2019-10-20")
