# Created by Hansi at 9/6/2019
import configparser
import csv
import json
import os
import time

import tweepy

import project_config
# from data_extract.tweet_util import get_hashtags

ITERATOR_LENGTH = 50
API_CALL_LIMIT = 180
TIME_LIMIT = 15 * 60

configParser = configparser.RawConfigParser()
# configFilePath = "../config.txt"
configParser.read(project_config.twitter_config_path)

CONSUMER_KEY = configParser.get('twitter-dev-configs', 'CONSUMER_KEY')
CONSUMER_SECRET = configParser.get('twitter-dev-configs', 'CONSUMER_SECRET')
OAUTH_TOKEN = configParser.get('twitter-dev-configs', 'OAUTH_TOKEN')
OAUTH_TOKEN_SECRET = configParser.get('twitter-dev-configs', 'OAUTH_TOKEN_SECRET')

# FULL_DATA_SET_PATH = '../data/full_dataset/'
FULL_DATA_SET_PATH = project_config.data_path + 'full_dataset/'


def get_tweet_by_id(id):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)
    tweet = api.get_status(id, tweet_mode="extended")

    print("Tweet_ID: " + str(id))
    print("Tweet: " + tweet.text)
    print("Timestamp: ", tweet.created_at)
    return tweet


# extract tweet summaries using hashtag, from_date and to_date
# date format - '2019-06-01'
# note - If to_date is givens as '2019-06-01', this collects tweets from '2019-06-01 23:59'
def get_tweet_summary_by_hashtag(hashtag, from_date, to_date):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)

    filename = hashtag.split('#')[1]

    # Open/Create a file to append data
    csvFile = open(FULL_DATA_SET_PATH + filename + '.tsv', 'a', newline='', encoding='utf-8')
    # Use csv Writer
    csvWriter = csv.writer(csvFile, delimiter='\t')

    # start_time = datetime.datetime.now()
    # i = 0
    for tweet in tweepy.Cursor(api.search, q=hashtag, count=100,
                               lang="en",
                               since=from_date, until=to_date, tweet_mode='extended').items():

        try:
            print(tweet.id_str, tweet.created_at, tweet.retweeted_status.full_text,
                  get_hashtags(tweet.entities.get('hashtags')))
            csvWriter.writerow([tweet.id_str, tweet.created_at, tweet.retweeted_status.full_text,
                                get_hashtags(tweet.entities.get('hashtags')),
                                tweet.user.location])
        except AttributeError:  # Not a Retweet
            print(tweet.id_str, tweet.created_at, tweet.full_text, get_hashtags(tweet.entities.get('hashtags')))
            csvWriter.writerow([tweet.id_str, tweet.created_at, tweet.full_text,
                                get_hashtags(tweet.entities.get('hashtags')),
                                tweet.user.location])

        # print('Sleeping for (seconds) : 1')
        time.sleep(1)
        # 450 call per 15 mins


# extract tweets using hashtag, from_date and to_date
def get_tweet_by_hashtag(hashtag, from_date, to_date):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)

    filename = hashtag.split('#')[1]

    output_data_path = os.path.join(FULL_DATA_SET_PATH, filename + ".json")

    for tweet in tweepy.Cursor(api.search, q=hashtag, count=100,
                               lang="en",
                               since=from_date, until=to_date, tweet_mode='extended').items():
        print(tweet.id_str, tweet.created_at)
        with open(output_data_path, 'a', encoding='utf-8') as f:
            f.write("%s\n" % json.dumps(tweet._json))
        time.sleep(1)
        # 450 call per 15 mins


# extract tweets of given hashtag between from and to dates with id less than max_id
# date format - '2019-06-01', id format - id as string
# note - If to_date is givens as '2019-06-01', this collects tweets from '2019-06-01 23:59'
def get_tweet_by_hashtag_with_maxid(hashtag, from_date, to_date, max_id):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)

    filename = hashtag.split('#')[1]

    # Open/Create a file to append data
    csv_file = open(FULL_DATA_SET_PATH + filename + '.tsv', 'a', newline='', encoding='utf-8')
    # Use csv Writer
    csv_writer = csv.writer(csv_file, delimiter='\t')

    for tweet in tweepy.Cursor(api.search, q=hashtag, count=100,
                               lang="en",
                               since=from_date, until=to_date, max_id=max_id, tweet_mode='extended').items():
        try:
            print(tweet.id_str, tweet.created_at, tweet.retweeted_status.full_text,
                  get_hashtags(tweet.entities.get('hashtags')))
            csv_writer.writerow([tweet.id_str, tweet.created_at, tweet.retweeted_status.full_text,
                                 get_hashtags(tweet.entities.get('hashtags')),
                                 tweet.user.location])
        except AttributeError:  # Not a Retweet
            print(tweet.id_str, tweet.created_at, tweet.full_text, get_hashtags(tweet.entities.get('hashtags')))
            csv_writer.writerow([tweet.id_str, tweet.created_at, tweet.full_text,
                                 get_hashtags(tweet.entities.get('hashtags')),
                                 tweet.user.location])

        # print('Sleeping for (seconds) : 1')
        time.sleep(1)


# extract tweets of given hashtag between from and to dates with id less than max_id and more than since_id
# date format - '2019-06-01', id format - id as string
# note - If to_date is givens as '2019-06-01', this collects tweets from '2019-06-01 23:59'
def get_tweet_by_hashtag_within_id_range(hashtag, from_date, to_date, max_id, since_id):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)

    filename = hashtag.split('#')[1]

    # Open/Create a file to append data
    csv_file = open(FULL_DATA_SET_PATH + filename + '.tsv', 'a', newline='', encoding='utf-8')
    # Use csv Writer
    csv_writer = csv.writer(csv_file, delimiter='\t')

    for tweet in tweepy.Cursor(api.search, q=hashtag, count=100,
                               lang="en",
                               since=from_date, until=to_date, max_id=max_id, since_id=since_id,
                               tweet_mode='extended').items():

        try:
            print(tweet.id_str, tweet.created_at, tweet.retweeted_status.full_text,
                  get_hashtags(tweet.entities.get('hashtags')))
            csv_writer.writerow([tweet.id_str, tweet.created_at, tweet.retweeted_status.full_text,
                                 get_hashtags(tweet.entities.get('hashtags')),
                                 tweet.user.location])
        except AttributeError:  # Not a Retweet
            print(tweet.id_str, tweet.created_at, tweet.full_text, get_hashtags(tweet.entities.get('hashtags')))
            csv_writer.writerow([tweet.id_str, tweet.created_at, tweet.full_text,
                                 get_hashtags(tweet.entities.get('hashtags')),
                                 tweet.user.location])

        # print('Sleeping for (seconds) : 1')
        time.sleep(1)


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
    # get_tweet_by_hashtag("#UCL", "2020-02-17", "2020-02-19")
    get_tweet_by_hashtag("#USelection", "2020-11-03", "2020-11-04")
    # get_tweet_by_hashtag_within_id_range("#ATLLIV", "2020-02-17", "2020-02-19", "1229881019790249986",
    #                                 "1229827921495216129")
    # get_tweet_by_hashtag_within_id_range("#BVBPSG", "2020-02-17", "2020-02-19", "1229881019790249986",
    #                                      "1229827921495216129")
    # get_tweet_by_hashtag_within_id_range("#BrexitDeal", "2019-10-18", "2019-10-20", "1185526625351409664",
    #                                      "1185433082599374849")

    # get_tweet_by_id(1185579083385724928)
    # get_tweet_by_hashtag("#ElectionResults2019", "2019-12-13", "2019-12-14")

    # get_tweet_by_hashtag_within_id_range("#MCFC", "2020-01-07", "2020-01-08", "1214630367837200389", "1214630282281664512")
