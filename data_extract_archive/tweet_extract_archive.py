# Created by Hansi at 10/13/2020

#
import bz2
import json
import os
import re
import zipfile

from data_extract.tweet_util import get_hashtag_list


# extract full text
# tweet_json - Json object of tweet
# include_rt - Boolean to indicate include retweet notation or not (RT @***:)
def get_full_text_and_hashtags(tweet_json, include_rt=False):
    hashtags = get_hashtag_list(tweet_json['entities']['hashtags'])
    full_text = tweet_json['text']
    print(full_text)
    # if tweet is a retweet, check truncate attribute within 'retweeted_status'
    if 'retweeted_status' in tweet_json.keys():
        # if truncated true, get full_text from extended_tweet within retweeted_status
        if tweet_json['retweeted_status']['truncated']:
            full_text = tweet_json['retweeted_status']['extended_tweet']['full_text']
            hashtags = get_hashtag_list(tweet_json['retweeted_status']['extended_tweet']['entities']['hashtags'])
        # if truncated false, get full_text in retweeted_status
        else:
            full_text = tweet_json['retweeted_status']['text']
            hashtags = get_hashtag_list(tweet_json['retweeted_status']['entities']['hashtags'])

        if include_rt:
            # add retweet notation to full_text
            retweet_substring = re.findall(r'^RT @[a-zA-Z0-9_/-]*:', tweet_json['text'])[0]
            full_text = retweet_substring + " " + full_text
    # if tweet is not a retweet
    else:
        # if truncated true, get full_text from extended_tweet
        if tweet_json['truncated']:
            full_text = tweet_json['extended_tweet']['full_text']
            hashtags = get_hashtag_list(tweet_json['extended_tweet']['entities']['hashtags'])
    return full_text, hashtags


# if both hashtags and tokens are given, filtered tweets should contain at least one hashtag AND one token
# return filtered tweet json/None and 0 (deleted/other language tweet)/1(available tweet)- used to get stat details
def filter_json(tweet_json, lang=None, hashtags=None, tokens=None, lowercase_match=True, include_rt=False):
    # check json corresponds to a deleted tweet
    if 'delete' in tweet_json.keys():
        return None, 0

    # filter by language, if required
    if lang:
        if tweet_json['lang'] != lang:
            return None, 0

    tweet_full_text, tweet_hashtags = get_full_text_and_hashtags(tweet_json, include_rt=include_rt)

    # filter by hashtags, if required (e.g. Education_kills_secondary)
    if hashtags:
        if lowercase_match:
            # convert hashtag list to lowercase
            tweet_hashtags = [token.lower() for token in tweet_hashtags]

        # keep the tweet if al least one hashtag is found
        if len(list(set(hashtags) & set(tweet_hashtags))) == 0:
            return None, 1

    # filter by tokens, if required
    if tokens:
        matched_token_count = 0
        tweet_text = tweet_full_text
        if lowercase_match:
            tweet_text = tweet_full_text.lower()
        for token in tokens:
            if token in tweet_text:
                matched_token_count += 1
        if matched_token_count == 0:
            return None, 1

    # add full_text attribute
    tweet_json['full_text'] = tweet_full_text
    tweet_json['hashtags'] = tweet_hashtags
    print(tweet_hashtags)

    return tweet_json, 1


def filter_data(folder_path, lang=None, hashtags=None, tokens=None, lowercase_match=True, output_file_path=None,
                include_rt=False):
    if lowercase_match:
        if hashtags:
            hashtags = [token.lower() for token in hashtags]
        if tokens:
            tokens = [token.lower() for token in tokens]

    output_file = None
    if output_file_path:
        output_file = open(output_file_path, 'w', encoding='utf-8')

    data = []
    tweet_count = 0
    for root, dirs, files in os.walk(folder_path):
        for dir in dirs:
            print('processing directory: ', dir)
            for sub_root, sub_dirs, sub_files in os.walk(os.path.join(folder_path, dir)):
                for file in sub_files:
                    file_path = os.path.join(folder_path, dir, file)
                    print('processing file: ', file)
                    with bz2.BZ2File(file_path, "r") as f:
                        for line in f:
                            tweet_json = json.loads(line)
                            filtered_tweet, count = filter_json(tweet_json, lang=lang, hashtags=hashtags, tokens=tokens,
                                                                lowercase_match=lowercase_match, include_rt=include_rt)
                            tweet_count += count
                            if filtered_tweet:
                                data.append(filtered_tweet)
                                if output_file_path:
                                    output_file.write("%s\n" % json.dumps(filtered_tweet))
    if output_file_path:
        output_file.close()

    print('tweet count: ', tweet_count)


def filter_data_multiple(folder_path, dict_tags, lang=None, lowercase_match=True, output_folder_path=None,
                         include_rt=False):
    if lowercase_match:
        for key in dict_tags.keys():
            hashtags = dict_tags[key][0]
            tokens = dict_tags[key][1]
            if hashtags:
                hashtags = [token.lower() for token in hashtags]
            if tokens:
                tokens = [token.lower() for token in tokens]
            dict_tags[key] = [hashtags, tokens]

    if output_folder_path:
        dict_output_files = dict()
        for key in dict_tags.keys():
            output_file = open(os.path.join(output_folder_path, str(key) + ".json"), 'w', encoding='utf-8')
            dict_output_files[key] = output_file

    dict_data = dict()
    for key in dict_tags.keys():
        dict_data[key] = []

    tweet_count = 0
    for root, dirs, files in os.walk(folder_path):
        for dir in dirs:
            print('processing directory: ', dir)
            for sub_root, sub_dirs, sub_files in os.walk(os.path.join(folder_path, dir)):
                for file in sub_files:
                    file_path = os.path.join(folder_path, dir, file)
                    print('processing file: ', file)
                    with bz2.BZ2File(file_path, "r") as f:
                        for line in f:
                            tweet_json = json.loads(line)
                            for key in dict_tags.keys():
                                filtered_tweet, count = filter_json(tweet_json, lang=lang, hashtags=dict_tags[key][0],
                                                                    tokens=dict_tags[key][1],
                                                                    lowercase_match=lowercase_match,
                                                                    include_rt=include_rt)
                                if filtered_tweet:
                                    dict_data[key].append(filtered_tweet)
                                    if output_folder_path:
                                        dict_output_files[key].write("%s\n" % json.dumps(filtered_tweet))
                            tweet_count += count
    if output_folder_path:
        for key in dict_output_files.keys():
            dict_output_files[key].close()

    print('tweet count: ', tweet_count)


# def test_folder(folder_path):
#     for root, dirs, files in os.walk(folder_path):
#         for dir in dirs:
#             print('processing directory: ', dir)
#             for sub_root, sub_dirs, sub_files in os.walk(os.path.join(folder_path, dir)):
#                 for file in sub_files:
#                     file_path = os.path.join(folder_path, dir, file)
#                     print(file_path)
#                     with bz2.BZ2File(file_path, "r") as z:
#                         for line in z:
#                             tweet_json = json.loads(line)
#                             print()
#             print()


def read_zip_jsonl_file(file_path):
    data = []
    with zipfile.ZipFile(file_path, "r") as z:
        for filename in z.namelist():
            print(filename)
            with z.open(filename) as f:
                for line in f:
                    data.append(json.loads(line))
    return data


if __name__ == "__main__":
    list = ["Abc", "DEf", "ghI"]
    list2 = ["Zvy", "hji"]
    lists = []
    lists.append(list)
    lists.append(lists)
    print(lists)

    # lists = [[token.lower() for token in hashtag_list] for hashtag_list in lists]
    # print(lists)
    dict_tags = dict()
    dict_tags[0] = [list, list2]
    print(dict_tags)

    for key in dict_tags.keys():
        hashtags = [token.lower() for token in dict_tags[key][0]]
        tokens = [token.lower() for token in dict_tags[key][1]]
        dict_tags[key] = [hashtags, tokens]

    print(dict_tags)
