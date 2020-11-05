# Created by Hansi at 10/29/2020
import json
import os

from data_extract.tweet_util import merge_without_duplicates_json
from data_extract_archive.tweet_info_extract import get_tweet_summary


# use_all_attributes - boolean - True: filter json which contains at least one of each given attribute values
# e.g. given hashtags and tokens, True > filter json if at least one given hashtag and token are available
def filter_extracted_json(tweet_json, hashtags=None, tokens=None, lowercase_match=True, use_all_attributes=True):
    tweet_full_text = tweet_json["full_text"]
    tweet_hashtags = tweet_json["hashtags"]

    fulfilled = False

    # filter by hashtags, if required (e.g. Education_kills_secondary)
    if hashtags:
        if lowercase_match:
            # convert hashtag list to lowercase
            tweet_hashtags = [token.lower() for token in tweet_hashtags]

        # keep the tweet if al least one hashtag is found
        if len(list(set(hashtags) & set(tweet_hashtags))) == 0:
            if use_all_attributes:
                return None, 1
        else:
            fulfilled = True

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
            if use_all_attributes:
                return None, 1
        else:
            fulfilled = True

    if fulfilled:
        return tweet_json, 1
    else:
        return None, 1


def filter_extracted_data(file_path, hashtags=None, tokens=None, lowercase_match=True, output_file_path=None,
                          removed_data_path=None):
    if lowercase_match:
        if hashtags:
            hashtags = [token.lower() for token in hashtags]
        if tokens:
            tokens = [token.lower() for token in tokens]

    output_file = None
    if output_file_path:
        output_file = open(output_file_path, 'w', encoding='utf-8')
    if removed_data_path:
        removed_data_file = open(removed_data_path, 'w', encoding='utf-8')

    data = []
    tweet_count = 0
    with open(file_path, 'r') as f:
        for line in f:
            tweet_json = json.loads(line)
            filtered_tweet, count = filter_extracted_json(tweet_json, hashtags=hashtags, tokens=tokens,
                                                lowercase_match=lowercase_match, use_all_attributes=False)
            if filtered_tweet:
                tweet_count += count
                data.append(filtered_tweet)
                if output_file_path:
                    output_file.write("%s\n" % json.dumps(filtered_tweet))
            else:
                if removed_data_path:
                    removed_data_file.write("%s\n" % json.dumps(tweet_json))

    if output_file_path:
        output_file.close()
    if removed_data_path:
        removed_data_file.close()

    print('filtered tweet count: ', tweet_count)


if __name__ == "__main__":
    date = "04/15"
    topic = "E2"

    base_folder = "F:/Twitter data"
    folder_path = os.path.join(base_folder, "filtered", date, topic)
    file_path = os.path.join(base_folder, "filtered", date, topic + ".json")
    filtered_data_path = os.path.join(base_folder, "filtered", date, topic + "_filtered.json")
    removed_data_path = os.path.join(base_folder, "filtered", date, topic + "_removed.json")
    filtered_data_summary_path = os.path.join(base_folder, "summary", date, topic + "_filtered.tsv")
    removed_data_summary_path = os.path.join(base_folder, "summary", date, topic + "_removed.tsv")

    # merge_without_duplicates_json(folder_path, file_path)

    # hashtags = ['olympictorchrelay', 'olympicflame']
    # tokens = ['torch', 'flame', 'relay']
    hashtags = None
    # tokens = ['postponement', 'decision', 'postpone', 'postponing', 'postponed']
    # tokens = ['postponement', 'decision']
    # tokens = ['tokyo', 'games', '2020', '2021', 'postponed', 'summer', 'ioc']
    # tokens = ['postponed']
    # tokens = ['2021', 'postponed', 'summer', 'ioc', 'july', '23', 'new', '8']
    tokens = ['2021', 'postponed', 'july', '23', 'new dates', '8', 'august']
    filter_extracted_data(file_path, hashtags=hashtags, tokens=tokens, output_file_path=filtered_data_path)

    get_tweet_summary(filtered_data_path, filtered_data_summary_path)
    # get_tweet_summary(removed_data_path, removed_data_summary_path)



