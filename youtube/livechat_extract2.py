import pytchat
import pandas as pd
import os


# video_id = 'hp1JcTV0R0E' # https://www.youtube.com/watch?v=hp1JcTV0R0E&ab_channel=TheIndependent
# video_id = '756VdjT3KwY'  # sky: https://www.youtube.com/watch?v=756VdjT3KwY
# video_id = '3dNAgCsq-Yc'  # https://www.youtube.com/watch?v=3dNAgCsq-Yc - ada derana
video_id = 'smb81JwOa2M'

folder_path = f'south_korea/{video_id}'
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# chat = pytchat.create(video_id=video_id)
#
# df = pd.DataFrame(columns=['id', 'comment', 'date', 'user_name', 'channelId', 'channelUrl',
#                            'userType', 'type'])
# i = 0
# n = 0
#
# while chat.is_alive():
#     for c in chat.get().sync_items():
#         print(f"{c.id}\t{c.datetime}\t{c.author.name}\t{c.message}")
#         df.loc[i] = [c.id, c.message, c.datetime, c.author.name, c.type]
#         i += 1
#
#         if i==100:
#             print(f'saving iteration {n}')
#             df.to_csv(f"{folder_path}/livechat_{n}.csv", index=False, encoding='utf-8')
#             n += 1
#             df = pd.DataFrame(columns=['id', 'comment', 'date', 'user_name', 'type'])
#             i = 0
#
# print()


# from pytchat import LiveChat
# import time
# chat = LiveChat(video_id = "YEj2RakIf4s")
#
# while chat.is_alive():
#   try:
#     data = chat.get()
#     items = data.items
#     for c in items:
#         print(f"{c.datetime} [{c.author.name}]- {c.message}")
#     time.sleep(3)
#   except KeyboardInterrupt:
#     chat.terminate()
#     break

chat = pytchat.create(video_id=video_id)

df = pd.DataFrame(columns=['id', 'comment', 'date', 'user_name', 'channelId', 'channelUrl',
                           'userType', 'type'])
i = 0
n = 0

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

        if i==20:
            print(f'saving iteration {n}')
            df.to_csv(f"{folder_path}/livechat_{n}.csv", index=False, encoding='utf-8')
            n += 1
            # df = pd.DataFrame(columns=['id', 'comment', 'date', 'user_name', 'type'])
            df = pd.DataFrame(columns=['id', 'comment', 'date', 'user_name', 'channelId', 'channelUrl',
                                       'userType', 'type'])
            i = 0

print()