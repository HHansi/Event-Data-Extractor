# Created by Hansi at 10/21/2019
import collections
import csv
import operator


def order_hashtags(input_filepath, output_filepath):
    csv_file = open(input_filepath, encoding='utf-8')
    csv_reader = csv.reader(csv_file, delimiter='\t')
    tag_dict = {}
    i = 0
    for row in csv_reader:
        i += 1
        # print('hashtags: ', row[3])
        hashtag_list = row[3].split(',')

        for tag in hashtag_list:
            if tag and not tag.isspace():
                print(tag)
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


if __name__ == "__main__":
    input_filepath = '../data/full_dataset/BrexitVote.tsv'
    output_filepath = '../data/full_dataset/BrexitVote-hashtags.txt'
    order_hashtags(input_filepath, output_filepath)
