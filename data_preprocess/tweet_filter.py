# Created by Hansi at 10/30/2019
import csv
import os


# method to filter missing text tweet ids in the input file
# input_file format - [id timestamp tweet_text hashtags location]
# output_file format - [id] (ids which have missing tweet text)
def filter_missing_text_tweet_id(input_filepath, output_filepath):
    csv_file = open(input_filepath, encoding='utf-8')
    csv_reader = csv.reader(csv_file, delimiter='\t')

    csv_file_out = open(output_filepath, 'a', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file_out, delimiter='\t')

    for row in csv_reader:
        print(row[2][-1:])
        if "…" == row[2][-1:]:
            print(row[0])
            csv_writer.writerow([row[0]])

# method to filter tweets with full text and missing text
# Further it groups missing tweets which share same content
# input_file format - [id timestamp tweet_text hashtags location]
# output_file_full format - [id timestamp tweet_text hashtags location] (contains tweet data which have full text)
# output_file_missing format - [id id1,id2,id3..] (single row contains tweet ids which share same content and 0th row
# is a representative id for the group)
def filter_missing_text_tweet(input_filepath, output_filepath_full, output_filepath_missing):
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


if __name__ == "__main__":
    input_folder = '../data/full_dataset/BrexitVote/event/not_completed'
    output_folder = '../data/full_dataset/BrexitVote/filtered_tweet'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for root, dirs, files in os.walk(input_folder):
        for file in files:
            file_name = os.path.splitext(file)[0]
            input_filepath = input_folder + "/" + file
            output_filepath_full = output_folder + "/" + file_name + "_full.tsv"
            output_filepath_missing = output_folder + "/" + "missing_id/" + file
            filter_missing_text_tweet(input_filepath, output_filepath_full, output_filepath_missing)
