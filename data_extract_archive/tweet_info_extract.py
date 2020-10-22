# Created by Hansi at 10/16/2020
import csv

from wnut.extract import read_json_line2


def get_tweet_text(input_file_path, output_file_path):
    csv_file = open(output_file_path, 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file, delimiter='\t')

    inputs = read_json_line2(input_file_path)
    outputs = []
    for input in inputs:
        text = input['full_text']
        outputs.append(text)
        # output_file.write("%s\n" % text)
        csv_writer.writerow([text])

    csv_file.close()


def get_tweet_summary(input_file_path, output_file_path, add_retweets=False):
    csv_file = open(output_file_path, 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file, delimiter='\t')

    inputs = read_json_line2(input_file_path)
    outputs = []
    for input in inputs:
        text = input['full_text']
        hashtags = input['hashtags']
        hashtag_text = ','.join(hashtags)
        retweet_count = 0
        if 'retweeted_status' in input.keys():
            retweet_count = input['retweeted_status']['retweet_count']
        csv_writer.writerow([input['id'], input['created_at'], text, hashtag_text, input['user']['location'], retweet_count])

    csv_file.close()

