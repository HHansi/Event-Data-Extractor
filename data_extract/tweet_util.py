# Created by Hansi at 10/21/2019
import collections
import csv
import operator
import os


# Method to order hashtags in input file by frequency
# input_file format - [id timestamp tweet_text hashtags location]
def order_hashtags(input_filepath, output_filepath):
    csv_file = open(input_filepath, encoding='utf-8')
    csv_reader = csv.reader(csv_file, delimiter='\t')
    tag_dict = {}
    i = 0
    for row in csv_reader:
        i += 1
        # print('hashtags: ', row[3])
        print(row[0])
        hashtag_list = row[3].split(',')

        for tag in hashtag_list:
            if tag and not tag.isspace():
                # print(tag)
                if tag in tag_dict:
                    count = tag_dict[tag]
                    tag_dict[tag] = count + 1
                else:
                    tag_dict[tag] = 1

    sorted_list = sorted(tag_dict.items(), key=operator.itemgetter(1))
    sorted_list.reverse()
    sorted_dict = collections.OrderedDict(sorted_list)

    output_file = open(output_filepath, 'a', encoding='utf-8')

    for key in sorted_dict.keys():
        val = tag_dict[key]
        print(key + " : " + str(val))
        output_file.write(key + " : " + str(val) + "\n")

    output_file.close()


# Method to merge data in given files and sort by id
# input_file format - [id timestamp tweet_text hashtags location]
# output_file format - [id timestamp tweet_text hashtags location]
def merge_full_missing_tweets(full_tweet_path, missing_tweet_path, output_path):
    full_tweet_file = open(full_tweet_path, encoding='utf-8')
    full_tweet_reader = csv.reader(full_tweet_file, delimiter='\t')

    missing_tweet_file = open(missing_tweet_path, encoding='utf-8')
    missing_tweet_reader = csv.reader(missing_tweet_file, delimiter='\t')

    output_tweet_file = open(output_path, 'a', newline='', encoding='utf-8')
    output_tweet_writer = csv.writer(output_tweet_file, delimiter='\t')

    tweet_data = []

    for row in full_tweet_reader:
        # if row[0].strip() != '' or not row[0].strip().isspace():
        if row[0] and row[0].strip() != '' and not row[0].strip().isspace():
            tweet_data.append(row)
    for row in missing_tweet_reader:
        # if row[0].strip() != '' or not row[0].strip().isspace():
        if row[0] and row[0].strip() != '' and not row[0].strip().isspace():
            tweet_data.append(row)

    # sort tweets by id
    tweet_data_sorted = sorted(tweet_data, key=lambda x: x[0])

    for row in tweet_data_sorted:
        output_tweet_writer.writerow(row)


# Method to count number of entries/ rows in given fiel
def get_counts(file_path):
    file = open(file_path, encoding='utf-8')
    reader = csv.reader(file, delimiter='\t')
    n = 0
    for row in reader:
        n += 1
    print(str(file) + " : ", n)
    return n


# extract additional ids in file1 compared to file2
def get_id_diff(file_path1, file_path2, output_path):
    file1 = open(file_path1, encoding='utf-8')
    file1_reader = csv.reader(file1, delimiter='\t')

    file2 = open(file_path2, encoding='utf-8')
    file2_reader = csv.reader(file2, delimiter='\t')

    output_file = open(output_path, 'a', newline='', encoding='utf-8')
    output_writer = csv.writer(output_file, delimiter='\t')

    file2_n = get_counts(file_path2)
    file1_n = get_counts(file_path1)

    print(file1.name + " : ", file1_n)
    print(file2.name + " : ", file2_n)

    for row1 in file1_reader:
        n = 0
        for row2 in file2_reader:
            n += 1
            if row1[0] == row2[0]:
                break
            else:
                if n == file2_n:
                    output_writer.writerow(row1)


# method to merge all files in given folder path without duplicate IDS and save to output file
# input_file format - [id timestamp tweet_text hashtags location]
# output_file format - [id timestamp tweet_text hashtags location]
def merge_without_duplicates(folder_path, output_file_path):
    data_dict = dict()
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = folder_path + "/" + file
            print(file)
            file_read = open(file_path, encoding='utf-8')
            file1_reader = csv.reader(file_read, delimiter='\t')

            for row in file1_reader:
                if row[0] not in data_dict.keys():
                    data_dict[row[0]] = row
                else:
                    print(row[0])

    output_file = open(output_file_path, 'a', newline='', encoding='utf-8')
    output_writer = csv.writer(output_file, delimiter='\t')

    # sort data by ID
    sorted_data_dict = collections.OrderedDict(sorted(data_dict.items()))

    for key in sorted_data_dict.keys():
        output_writer.writerow(sorted_data_dict[key])


if __name__ == "__main__":
    # # order hashtags
    # input_filepath = '../data/full_dataset/RWC2019/RWC2019.tsv'
    # output_filepath = '../data/full_dataset/RWC2019/RWC2019-hashtags.txt'
    # order_hashtags(input_filepath, output_filepath)

    # # merge full and missing tweets
    # input_folder_full = '../data/full_dataset/BrexitVote/event_full_dataset/full-text'
    # input_folder_missing = '../data/full_dataset/BrexitVote/event_full_dataset/missing-text-extract'
    # output_folder = '../data/full_dataset/BrexitVote/event_full_dataset/merge'
    #
    # for root, dirs, files in os.walk(input_folder_full):
    #     for file in files:
    #         input_full = input_folder_full + "/" + file
    #         input_missing = input_folder_missing + "/" + file
    #         output_file = output_folder + "/" + file
    #
    #         merge_full_missing_tweets(input_full, input_missing, output_file)

    # # printing row counts
    # folder = '../data/full_dataset/BrexitVote/event_full_dataset/merge'
    # for root, dirs, files in os.walk(folder):
    #     for file in files:
    #         file_path = folder + "/" + file
    #         get_counts(file_path)
    #
    # print()
    #
    # folder = '../data/full_dataset/BrexitVote/event'
    # for root, dirs, files in os.walk(folder):
    #     for file in files:
    #         file_path = folder + "/" + file
    #         get_counts(file_path)

    # # get id diff
    # folder1_path = '../data/full_dataset/BrexitVote/event'
    # folder2_path = '../data/full_dataset/BrexitVote/event_full_dataset/merge'
    # output_path = '../data/full_dataset/BrexitVote/event_full_dataset/missing_text_extract_missed_ids'
    #
    # for root, dirs, files in os.walk(folder1_path):
    #     for file in files:
    #         print(file)
    #         file1_path = folder1_path + "/" + file
    #         file2_path = folder2_path + "/" + file
    #         outputfile_path = output_path + "/" + file
    #
    #         get_id_diff(file1_path, file2_path, outputfile_path)

    # merge without duplicate ids
    input_folder_path = '../data/full_dataset/BrexitVote/event_full_dataset/merge'
    output_file_path = '../data/full_dataset/BrexitVote/event_full_dataset/dataset.tsv'
    merge_without_duplicates(input_folder_path, output_file_path)
