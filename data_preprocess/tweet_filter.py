# Created by Hansi at 10/30/2019
import csv
from datetime import datetime


# method to filter missing text tweet ids in the input file
# input_file format - [id timestamp tweet_text hashtags location]
# output_file format - [id] (ids which have missing tweet text)
def filter_missing_text_tweet(input_filepath, output_filepath_full, output_filepath_missing):
    csv_file = open(input_filepath, encoding='utf-8')
    csv_reader = csv.reader(csv_file, delimiter='\t')

    csv_file_out_full = open(output_filepath_full, 'a', newline='', encoding='utf-8')
    csv_writer_full = csv.writer(csv_file_out_full, delimiter='\t')

    csv_file_out_missing = open(output_filepath_missing, 'a', newline='', encoding='utf-8')
    csv_writer_missing = csv.writer(csv_file_out_missing, delimiter='\t')

    for row in csv_reader:
        print(row[2][-1:])
        if "…" == row[2][-1:]:
            print(row[0])
            csv_writer_missing.writerow(row)
        else:
            csv_writer_full.writerow(row)


# method to filter tweets with full text and missing text
# Further it groups missing tweets which share same content
# input_file format - [id timestamp tweet_text hashtags location]
# output_file_full format - [id timestamp tweet_text hashtags location] (contains tweet data which have full text)
# output_file_missing format - [id id1,id2,id3..] (single row contains tweet ids which share same content and 0th row
# is a representative id for the group)
def filter_missing_text_tweet_ids(input_filepath, output_filepath_full, output_filepath_missing):
    csv_file = open(input_filepath, encoding='utf-8')
    csv_reader = csv.reader(csv_file, delimiter='\t')

    csv_file_out_full = open(output_filepath_full, 'a', newline='', encoding='utf-8')
    csv_writer_full = csv.writer(csv_file_out_full, delimiter='\t')

    csv_file_out_missing = open(output_filepath_missing, 'a', newline='', encoding='utf-8')
    csv_writter_missing = csv.writer(csv_file_out_missing, delimiter='\t')

    tweet_dict = dict()
    n_all = 0
    n_full = 0
    n_missing = 0
    for row in csv_reader:
        n_all += 1
        if "…" == row[2][-1:]:
            n_missing += 1
            if row[2] in tweet_dict.keys():
                id_list = tweet_dict[row[2]]
                id_list.append(row[0])
                tweet_dict[row[2]] = id_list
            else:
                id_list = [row[0]]
                tweet_dict[row[2]] = id_list
        else:
            n_full += 1
            csv_writer_full.writerow(row)

    for key in tweet_dict.keys():
        ids = ''
        for id in tweet_dict[key][1:]:
            if ids == '':
                ids = str(id)
            else:
                ids = ids + "," + str(id)

        csv_writter_missing.writerow([tweet_dict[key][0], ids])

    print('all tweet count : ', n_all)
    print('full tweet count : ', n_full)
    print('missed tweet count : ', n_missing)


# Method to filter tweets with empty content ('_na_')
# input_file/output_file format - [id timestamp tweet_text hashtags location]
def filter_empty_tweets(input_filepath, output_filepath_tweets, output_filepath_empty_tweets):
    csv_file = open(input_filepath, encoding='utf-8')
    csv_reader = csv.reader(csv_file, delimiter='\t')

    csv_file_out_tweets = open(output_filepath_tweets, 'a', newline='', encoding='utf-8')
    csv_writer_tweets = csv.writer(csv_file_out_tweets, delimiter='\t')

    csv_file_out_empty_tweets = open(output_filepath_empty_tweets, 'a', newline='', encoding='utf-8')
    csv_writer_empty_tweets = csv.writer(csv_file_out_empty_tweets, delimiter='\t')

    for row in csv_reader:
        if row[1] == '_na_':
            csv_writer_empty_tweets.writerow(row)
        else:
            csv_writer_tweets.writerow(row)


# Filter tweets in input file which belong to the time period starts from from_time_str and ends from to_time_str
# input and output format - [id, timestamp, tweet text, hashtags, user location] tsv file
# from_time_str and to_time_str - String in format '%Y_%m_%d_%H_%M_%S' (e.g. '2019_10_19_8_30_00')
def filter_tweets_by_time(input_filepath, from_time_str, to_time_str, output_filepath=None):
    input_file = open(input_filepath, encoding='utf-8')
    input_reader = csv.reader(input_file, delimiter='\t')
    n = 0

    if output_filepath:
        output_file = open(output_filepath, 'a', newline='', encoding='utf-8')
        output_writer = csv.writer(output_file, delimiter='\t')

    from_time = datetime.strptime(from_time_str, '%Y_%m_%d_%H_%M_%S')
    to_time = datetime.strptime(to_time_str, '%Y_%m_%d_%H_%M_%S')

    for row in input_reader:
        if row[1] != "_na_":
            time = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
            if (time >= from_time) and (time <= to_time):
                n += 1
                if output_filepath:
                    output_writer.writerow(row)
    return n


# Filter tweets which contains the given word phrase without considering case
def filter_tweets_by_text(input_filepath, word_phrase, output_filepath=None, ):
    input_file = open(input_filepath, encoding='utf-8')
    input_reader = csv.reader(input_file, delimiter='\t')
    n = 0

    if output_filepath:
        output_file = open(output_filepath, 'a', newline='', encoding='utf-8')
        output_writer = csv.writer(output_file, delimiter='\t')

    for row in input_reader:
        if word_phrase.lower() in row[2].lower():
            n += 1
            if output_filepath:
                output_writer.writerow(row)

    return n


# Filter tweets by word phrase (case insensitive) and time period
# from_time_str and to_time_str - String in format '%Y_%m_%d_%H_%M_%S' (e.g. '2019_10_19_8_30_00')
def filter_tweets_by_time_and_text(input_filepath, from_time_str, to_time_str, word_phrase, output_filepath=None):
    input_file = open(input_filepath, encoding='utf-8')
    input_reader = csv.reader(input_file, delimiter='\t')
    n = 0

    if output_filepath:
        output_file = open(output_filepath, 'a', newline='', encoding='utf-8')
        output_writer = csv.writer(output_file, delimiter='\t')

    from_time = datetime.strptime(from_time_str, '%Y_%m_%d_%H_%M_%S')
    to_time = datetime.strptime(to_time_str, '%Y_%m_%d_%H_%M_%S')

    for row in input_reader:
        if row[1] != "_na_":
            time = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
            if (time >= from_time) and (time <= to_time) and (word_phrase.lower() in row[2].lower()):
                n += 1
                if output_filepath:
                    output_writer.writerow(row)

    return n


if __name__ == "__main__":
    # input_folder = '../data/full_dataset/BrexitVote/event/completed'
    # output_folder = '../data/full_dataset/BrexitVote/event_full_dataset'
    # if not os.path.exists(output_folder):
    #     os.makedirs(output_folder)
    #
    # for root, dirs, files in os.walk(input_folder):
    #     for file in files:
    #         file_name = os.path.splitext(file)[0]
    #         input_filepath = input_folder + "/" + file
    #         output_filepath_full = output_folder + "/full-text/" + file
    #         output_filepath_missing = output_folder + "/missing-text/" + file
    #         filter_missing_text_tweet(input_filepath, output_filepath_full, output_filepath_missing)

    input_filepath = '../data/full_dataset/BrexitVote/event_full_dataset/full_dataset.tsv'
    output_filepath = '../data/full_dataset/BrexitVote/event_full_dataset/dataset-'

    word_phrase = 'Welsh'
    print(word_phrase)
    print(filter_tweets_by_text(input_filepath, word_phrase))

    from_time = '2019_10_19_9_30_00'
    to_time = '2019_10_19_10_00_59'
    print(from_time + "-" + to_time)
    output_filepath1 = output_filepath + word_phrase + "-" + from_time + "-" + to_time + ".tsv"
    output_filepath2 = output_filepath +from_time + "-" + to_time + ".tsv"
    print(filter_tweets_by_time(input_filepath, from_time, to_time))
    print(filter_tweets_by_time_and_text(input_filepath, from_time, to_time, word_phrase, output_filepath1))






