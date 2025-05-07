import argparse
import os
import time

import pandas as pd
import pytchat

reattempts = 5


class LiveChat:
    def __init__(self, video_id, folder):
        self.video_id = video_id
        self.folder = folder
        self.file_n = 0
        self.save_by = 100

        self.folder_path = f'{self.folder}/{video_id}'
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)

    def extract(self):
        chat = pytchat.create(video_id=self.video_id)

        df = pd.DataFrame(columns=['id', 'comment', 'date', 'user_name', 'channelId', 'channelUrl',
                                   'userType', 'type'])
        i = 0

        while chat.is_alive():
            for c in chat.get().sync_items():
                user_type = []
                if c.author.isChatOwner:
                    user_type.append('ChatOwner')
                if c.author.isChatSponsor:
                    user_type.append('ChatSponsor')
                if c.author.isChatModerator:
                    user_type.append('ChatModerator')
                if c.author.isVerified:
                    user_type.append('Verified')

                print(f"{c.id}\t{c.datetime}\t{c.author.name}\t{c.message}\t{c.author.channelId}")
                df.loc[i] = [c.id, c.message, c.datetime, c.author.name, c.author.channelId,
                             c.author.channelUrl, user_type, c.type]
                i += 1

                if i == self.save_by:
                    print(f'saving iteration {self.file_n}')
                    df.to_csv(f"{self.folder_path}/livechat_{self.file_n}.csv", index=False, encoding='utf-8')
                    self.file_n += 1
                    df = pd.DataFrame(columns=['id', 'comment', 'date', 'user_name', 'channelId', 'channelUrl',
                                               'userType', 'type'])
                    i = 0

        df.to_csv(f"{self.folder_path}/livechat_{self.file_n}.csv", index=False, encoding='utf-8')
        self.file_n += 1


parser = argparse.ArgumentParser()
parser.add_argument("-vid", type=str, help="video id")
parser.add_argument("-folder", type=str, help="folder name")

args = parser.parse_args()
livechat = LiveChat(args.vid, args.folder)
livechat.extract()

for i in range(reattempts):
    livechat.extract()
    time.sleep(10)


# if __name__ == '__main__':
#     video_id = 'vDsoWMOFafk'
#     main_folder = 'conclave_25'
#
#
#     extract(video_id, main_folder)
#
#     for i in range(reattempts):
#         extract(video_id, main_folder)
#         time.sleep(10)
