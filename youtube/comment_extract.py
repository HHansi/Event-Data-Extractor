# commentThreads documentation: https://developers.google.com/resources/api-libraries/documentation/youtube/v3/python/latest/youtube_v3.commentThreads.html
# commentThreads resource: https://developers.google.com/youtube/v3/docs/commentThreads#resource-representation
# comments resource: https://developers.google.com/youtube/v3/docs/comments#resource-representation

from googleapiclient.discovery import build
import pandas as pd
from time import sleep
import traceback
import json

from youtube import config


youtube = build('youtube', 'v3', developerKey=config.API_KEY)

video_id = 'YhvLcy8R9Tg'  # 'Hm0KMbjwf-8' # https://www.youtube.com/watch?v=YhvLcy8R9Tg&t=26s&ab_channel=DailyMail
max_results = 2  # default=20

request = youtube.commentThreads().list(
    part="snippet,replies",
    videoId=video_id,
    textFormat="plainText",
    maxResults=max_results)

df = pd.DataFrame(columns=['id', 'comment', 'replies', 'date', 'user_name', 'likes'])

while request:
    ids = []
    replies = []
    comments = []
    dates = []
    user_names = []
    likes = []

    try:
        response = request.execute()

        for item in response['items']:
            # Extracting comments
            id = item['snippet']['topLevelComment']['id']
            ids.append(id)

            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comments.append(comment)

            user_name = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
            user_names.append(user_name)

            date = item['snippet']['topLevelComment']['snippet']['publishedAt']
            dates.append(date)

            like = item['snippet']['topLevelComment']['snippet']['likeCount']
            likes.append(like)

            # counting number of reply of comment
            replycount = item['snippet']['totalReplyCount']

            # if reply is there
            if replycount > 0:
                # append empty list to replies
                replies.append([])
                # iterate through all reply
                for reply in item['replies']['comments']:
                    # Extract reply
                    reply_id = reply['id']
                    reply_text = reply['snippet']['textDisplay']
                    reply_user_name = reply['snippet']['authorDisplayName']
                    reply_date = reply['snippet']['publishedAt']
                    reply_like = reply['snippet']['likeCount']

                    # append reply to last element of replies
                    # replies[-1].append(reply)
                    replies[-1].append({"reply_id": reply_id, "reply_text": reply_text, "user_name": reply_user_name, "date": reply_date, "likes": reply_like})
            else:
                replies.append([])

        # create new dataframe
        df2 = pd.DataFrame({"id": ids, "comment": comments, "replies": replies, "user_name": user_names, "date": dates, "likes": likes})
        df = pd.concat([df, df2], ignore_index=True)
        df['replies'] = df['replies'].apply(json.dumps)

        df.to_csv(f"{video_id}_user_comments.csv", index=False, encoding='utf-8')
        sleep(2)
        # request = youtube.commentThreads().list_next(request, response)
        # print("Iterating through next page")
        break
    except Exception as e:
        print(str(e))
        print(traceback.format_exc())
        print("Sleeping for 10 seconds")
        sleep(10)
        df.to_csv(f"{video_id}_user_comments.csv", index=False, encoding='utf-8')
        break

