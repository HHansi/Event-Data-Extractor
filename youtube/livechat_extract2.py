import pytchat
import pandas as pd
import os


video_id = 'hp1JcTV0R0E'

if not os.path.exists(video_id):
    os.makedirs(video_id)

chat = pytchat.create(video_id=video_id)

df = pd.DataFrame(columns=['id', 'comment', 'date', 'user_name', 'type'])
i = 0
n = 0

while chat.is_alive():
    for c in chat.get().sync_items():
        print(f"{c.id}\t{c.datetime}\t{c.author.name}\t{c.message}")
        df.loc[i] = [c.id, c.message, c.datetime, c.author.name, c.type]
        i += 1

        if i==100:
            print(f'saving iteration {n}')
            df.to_csv(f"{video_id}/livechat_{n}.csv", index=False, encoding='utf-8')
            n += 1
            df = pd.DataFrame(columns=['id', 'comment', 'date', 'user_name', 'type'])
            i = 0

print()


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