# Created by Hansi at 10/13/2020
import os

from data_extract.tweet_util import merge_without_duplicates, merge_without_duplicates_json
from data_extract_archive.tweet_extract_archive import filter_data, filter_data_multiple
from data_extract_archive.tweet_info_extract import get_tweet_summary
from util.file_util import create_folder_if_not_exist
from util.tweet_util import order_hashtags


def single_process():
    input_folder = "F:/Twitter data/input"
    filtered_folder = "F:/Twitter data/filtered"
    summary_folder = "F:/Twitter data/summary"

    date = "03/15"
    topic = "E4"
    id = "0"
    folder_path = os.path.join(input_folder, date)

    # Filter data
    include_rt = True
    lang = 'en'
    hashtags = ['coronavirus', 'covid19']
    tokens = ['vaccine']
    # hashtags = None
    # tokens = ['cyclone harold', 'harold cyclone', 'cyclone']



    filtered_data_path = os.path.join(filtered_folder, date, topic, id + ".json")
    create_folder_if_not_exist(filtered_data_path, is_file_path=True)
    filter_data(folder_path, lang=lang, hashtags=hashtags, tokens=tokens, output_file_path=filtered_data_path,
                include_rt=include_rt)

    # Extract important details
    data_output_folder = os.path.join(summary_folder, date, topic, 'data')
    output_file_path = os.path.join(data_output_folder, id + ".tsv")
    create_folder_if_not_exist(output_file_path, is_file_path=True)
    get_tweet_summary(filtered_data_path, output_file_path)

    # order hashtags by frequency
    hashtag_file = os.path.join(summary_folder, date, topic, 'hashtags', id + ".txt")
    create_folder_if_not_exist(hashtag_file, is_file_path=True)
    order_hashtags(output_file_path, hashtag_file)

    # merge files into single tweet data file (.tsv)
    # final_data_file = os.path.join(summary_folder, date, topic, 'data.tsv')
    # merge_without_duplicates(data_output_folder, final_data_file)

    # merge files into single tweet data file (.json)
    json_data_file = os.path.join(filtered_folder, date, topic + ".json" )
    merge_without_duplicates_json(folder_path, json_data_file)
    summary_data_file = os.path.join(summary_folder, date, topic + ".tsv")
    # get_tweet_summary(json_data_file, summary_data_file)


def multiple_process():
    input_folder = "F:/Twitter data/input"
    filtered_folder = "F:/Twitter data/filtered"
    summary_folder = "F:/Twitter data/summary"

    date = "03/15"
    topic = "E3"
    folder_path = os.path.join(input_folder, date)

    # Filter data
    include_rt = True
    lang = 'en'
    dict_tags = dict()
    hashtags = ['cycloneharold', 'harold', 'tcharold']
    tokens = None
    dict_tags[0] = [hashtags, tokens]

    hashtags = None
    tokens = ['cyclone harold', 'harold cyclone']
    dict_tags[1] = [hashtags, tokens]

    # hashtags = ['coronavirus', 'covid19', 'coronalockdown', 'coronavirusoutbreak', 'covid19uk', 'covid2019']
    # tokens = ['boris johnson', 'uk prime minister', 'british prime minister']
    # dict_tags[2] = [hashtags, tokens]

    filtered_folder_path = os.path.join(filtered_folder, date, topic)
    create_folder_if_not_exist(filtered_folder_path)
    filter_data_multiple(folder_path, dict_tags, lang=lang,  output_folder_path=filtered_folder_path,
                include_rt=include_rt)

    # Extract important details
    data_output_folder = os.path.join(summary_folder, date, topic, 'data')
    for key in dict_tags:
        output_file_path = os.path.join(data_output_folder, str(key) + ".tsv")
        filtered_data_path = os.path.join(filtered_folder_path, str(key) + ".json")
        create_folder_if_not_exist(output_file_path, is_file_path=True)
        get_tweet_summary(filtered_data_path, output_file_path)

    # order hashtags by frequency
    for key in dict_tags:
        hashtag_file = os.path.join(summary_folder, date, topic, 'hashtags', str(key) + ".txt")
        output_file_path = os.path.join(data_output_folder, str(key) + ".tsv")
        create_folder_if_not_exist(hashtag_file, is_file_path=True)
        order_hashtags(output_file_path, hashtag_file)

    # merge files into single tweet data file (.tsv)
    # final_data_file = os.path.join(summary_folder, date, topic, 'data.tsv')
    # merge_without_duplicates(data_output_folder, final_data_file)

    # merge files into single tweet data file (.json)
    json_data_file = os.path.join(filtered_folder, date, topic + ".json" )
    merge_without_duplicates_json(filtered_folder_path, json_data_file)
    summary_data_file = os.path.join(summary_folder, date, topic + ".tsv")
    get_tweet_summary(json_data_file, summary_data_file)


if __name__ == "__main__":
    # multiple_process()
    single_process()
