import json
import os
import googleapiclient.discovery
from datetime import datetime
import awswrangler as wr


def lambda_handler(event, context):
    try:
        '''Connect to youtube api using your credentials'''
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        api_service_name = "youtube"
        api_version = "v3"
        developer_key= os.environ['youtubekey']
        youtube = googleapiclient.discovery.build \
        (api_service_name, api_version, developerKey = developer_key)
        
        #getting videos from the following regions
        locations=['DE','AT','CH','FR']
        for location in locations:
            '''Get top 10 trending videos of youtube'''
            request_videos= youtube.videos().list(part="snippet,contentDetails,statistics,topicDetails",
                                                        chart="mostPopular",
                                                        maxResults=10,
                                                        regionCode=location)
            response_videos = request_videos.execute()
            #Finally writing this to a json file in s3 bucket
            #This will write daily trending videos in a separate bucket according to dates
            today=datetime.today().date()
            s3_bucket = 'youtube-etl-raw'
            s3_key = f'videos/regioncode={location}/date={today}/videos.json'
            jsonfp=f"/tmp/videos.json"
            with open(jsonfp,'w',encoding='utf-8') as json_file:
                json.dump(response_videos,json_file,indent=2)        
            wr.s3.upload(jsonfp, path=f's3://{s3_bucket}/{s3_key}')
    except Exception as e:
        print(e)
        raise(e)
    
