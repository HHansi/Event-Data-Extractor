# Created by Hansi at 10/19/2020


import collections
import csv
import operator


# Method to order hashtags in input file by frequency
# input_file format - [id timestamp tweet_text hashtags location retweet_count]
# retweet_count need to be available only if use_retweets=True
def order_hashtags(input_filepath, output_filepath, use_retweets=False):
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
                addition = 1
                if use_retweets:
                    addition = int(row[5])
                # print(tag)
                if tag in tag_dict:
                    count = tag_dict[tag]
                    tag_dict[tag] = count + addition
                else:
                    tag_dict[tag] = addition

    sorted_list = sorted(tag_dict.items(), key=operator.itemgetter(1))
    sorted_list.reverse()
    sorted_dict = collections.OrderedDict(sorted_list)

    output_file = open(output_filepath, 'a', encoding='utf-8')

    for key in sorted_dict.keys():
        val = tag_dict[key]
        print(key + " : " + str(val))
        output_file.write(key + " : " + str(val) + "\n")

    output_file.close()


def get_hashtags(json_array):
    hashtag_text = ''
    for item in json_array:
        if hashtag_text == '':
            hashtag_text = item['text']
        else:
            hashtag_text = hashtag_text + "," + item['text']
    return hashtag_text


def get_hashtag_list(json_array):
    hashtags = []
    for item in json_array:
        hashtags.append(item['text'])
    return hashtags

