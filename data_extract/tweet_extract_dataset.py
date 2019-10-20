# Created by Hansi at 9/6/2019
import configparser
import datetime
import os
import time

import pandas as pd
import tweepy
from tweet_extract import get_hashtags

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


def extract_dataset(dataset_id):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)

    start_time = datetime.datetime.now()

    if dataset_id == 1:
        # read dataset1 Tweet IDs
        df_all = pd.read_csv("../data/dataset1/relevant_tweets.tsv", sep='\t', names=['event', 'tweet_id'],
                             encoding='utf-8')
        print("data shape: ", df_all.shape)
        # print("data head: ", df_all.head(10))

        output_file = "../data/full_dataset/dataset" + str(dataset_id) + "/extracted_data.tsv"

        # add column names
        df_extracted = pd.DataFrame(columns=['Event', 'Tweet_Id', 'Tweet', 'HashTags'])
        df_extracted.to_csv(output_file, sep='\t', mode='a', index=False)
        df_extracted = df_extracted[:-1]

        i = 0
        for index, row in df_all.iterrows():
            i = i + 1
            tweet_id = row['tweet_id']
            try:
                tweet = api.get_status(tweet_id)
                df_extracted = df_extracted.append({'Event': row['event'], 'Tweet_Id': tweet_id, 'Tweet': tweet.text,
                                                    'HashTags': get_hashtags(tweet.entities.get('hashtags'))},
                                                   ignore_index=True)
            except tweepy.TweepError:
                print('Error occurred while retrieving Tweet: ' + str(tweet_id))
                df_extracted = df_extracted.append({'Event': row['event'], 'Tweet_Id': tweet_id, 'Tweet': 'NA',
                                                    'HashTags': 'NA'}, ignore_index=True)

            # save to file when ITERATOR_LENGTH found
            if i == ITERATOR_LENGTH:
                df_extracted.to_csv(output_file, sep='\t', mode='a', index=False,
                                    header=False, encoding='utf-8')
                i = 0
                df_extracted = df_extracted[:-ITERATOR_LENGTH]

    elif dataset_id == 2:
        root_path = "../data/dataset" + str(dataset_id)

        for root, dirs, files in os.walk(root_path):
            if 'ids' in root:
                folder_path = root + "/" + "selected"
                category = os.path.basename(root)

                for sub_root, sub_dirs, sub_files in os.walk(folder_path):
                    output_file_init = "../data/full_dataset/dataset" + str(
                        dataset_id) + "/" + category

                    for file in sub_files:
                        file_path = folder_path + "/" + file
                        category = os.path.splitext(file)[0]

                        output_file = output_file_init + "_extracted_data_" + category + ".tsv"

                        # add column names
                        df_extracted = pd.DataFrame(columns=['Time_Cat', 'Tweet_Id', 'Tweet', 'HashTags'])
                        df_extracted.to_csv(output_file, sep='\t', mode='a', index=False)
                        df_extracted = df_extracted[:-1]

                        with open(file_path) as f:
                            lines = f.readlines()

                            i = 0
                            for line in lines:
                                i = i + 1
                                tweet_id = line

                                # save to file when API call limit found
                                if i == API_CALL_LIMIT:
                                    df_extracted.to_csv(
                                        output_file, sep='\t', mode='a', index=False, header=False, encoding='utf-8')
                                    i = 0
                                    df_extracted = df_extracted[:-API_CALL_LIMIT]

                                    current_time = datetime.datetime.now()
                                    time_diff = current_time - start_time

                                    if time_diff.seconds < TIME_LIMIT:
                                        print('Sleeping for (seconds) : ', time_diff.seconds)
                                        time.sleep(TIME_LIMIT - time_diff.seconds)

                                try:
                                    tweet = api.get_status(tweet_id)
                                    df_extracted = df_extracted.append(
                                        {'Time_Cat': category, 'Tweet_Id': str(tweet_id), 'Tweet': tweet.text,
                                         'HashTags': get_hashtags(tweet.entities.get('hashtags'))}, ignore_index=True)
                                except tweepy.TweepError:
                                    print('Error occurred while retrieving Tweet: ' + str(tweet_id))
                                    df_extracted = df_extracted.append(
                                        {'Time_Cat': category, 'Tweet_Id': tweet_id, 'Tweet': 'NA', 'HashTags': 'NA'},
                                        ignore_index=True)


if __name__ == "__main__":
    extract_dataset(2)
