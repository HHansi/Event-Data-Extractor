from googleapiclient.discovery import build
import pandas as pd
from time import sleep
import traceback
import json

from youtube import config


youtube = build('youtube', 'v3', developerKey=config.API_KEY)

video_id = 'YhvLcy8R9Tg'
max_results = 2  # default=20

comment_df = pd.read_csv(f"{video_id}_user_comments.csv")
comment_df['replies'] = comment_df['replies'].apply(json.loads)
comment_df = comment_df[comment_df['replies'].apply(lambda x: len(x) != 0)]
parent_ids = comment_df["id"].tolist()

df = pd.DataFrame(columns=['parent_id', 'id', 'reply', 'user_name', 'date', 'likes'])

for parent_id in parent_ids:
    print()
    request = youtube.comments().list(
        part="snippet",
        parentId=parent_id,
        textFormat="plainText",
        maxResults=max_results)

    while request:
        ids = []
        replies = []
        dates = []
        user_names = []
        likes = []

        try:
            response = request.execute()

            for item in response['items']:
                # Extracting comments
                id = item['id']
                ids.append(id)

                reply = item['snippet']['textDisplay']
                replies.append(reply)

                user_name = item['snippet']['authorDisplayName']
                user_names.append(user_name)

                date = item['snippet']['publishedAt']
                dates.append(date)

                like = item['snippet']['likeCount']
                likes.append(like)

            parent_ids = [parent_id for i in range(len(ids))]
            # create new dataframe
            df2 = pd.DataFrame({"parent_id": parent_ids, "id": ids, "reply": replies, "user_name": user_names, "date": dates, "likes": likes})
            df = pd.concat([df, df2], ignore_index=True)

            df.to_csv(f"{video_id}_user_replies.csv", index=False, encoding='utf-8')
            sleep(2)
            request = youtube.comments().list_next(request, response)
            print("Iterating through next page")
            break
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())
            print("Sleeping for 10 seconds")
            sleep(10)
            df.to_csv(f"{video_id}_user_replies.csv", index=False, encoding='utf-8')
            break