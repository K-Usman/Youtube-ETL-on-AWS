import awswrangler as wr
import pandas as pd
import os
import json
import boto3

os_input_s3_parquetbucket = os.environ['s3_parquetbucket']
os_input_catalog_raw_db = os.environ['catalog_raw_db']
os_input_catalog_raw_table = os.environ['catalog_raw_table']
os_input_write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    try:
        
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
    
         # Read the JSON data from the S3 object
        json_data = response['Body'].read().decode('utf-8')
    
        # Now, you can load the JSON data
        data = json.loads(json_data)
        # Creating DF from content

        # Extract required columns:
        df_step_1 = pd.json_normalize(data['items'])

        # Write to S3
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_parquetbucket,
            dataset=True,
            database=os_input_catalog_raw_db,
            table=os_input_catalog_raw_table,
            mode=os_input_write_data_operation
        )

        return wr_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e