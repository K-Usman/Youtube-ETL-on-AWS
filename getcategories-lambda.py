import json
import os
import googleapiclient.discovery
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
        
        '''Get all categories of youtube'''
        request_categories = youtube.videoCategories().list(part="snippet",regionCode='DE') #specifying one country assuming every country has same categories
        response_categories = request_categories.execute()
        json_string = json.dumps(response_categories)

        s3_bucket = 'youtube-etl-raw'
        s3_key = 'categories/allcategories.json'

        jsonfp="/tmp/allcategories.json"
        with open(jsonfp,'w') as json_file:
            json_file.write(json_string)
        
        wr.s3.upload(jsonfp, path=f's3://{s3_bucket}/{s3_key}')
    
    except Exception as e:
        print(e)
        raise(e)
    
