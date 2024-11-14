import pandas as pd
import boto3
from io import BytesIO
import json
import os

# Get the directory path of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to config.json
config_file_path = os.path.join(current_dir, 'config.json')

def read_config(filename=config_file_path):
    print('Reading config.json ....')
    with open(filename, 'r') as f:
        return json.load(f)

def read_from_s3(bucket_name, key, aws_config):
    print('Reading s3 file.....')
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_config['access_key'],
                      aws_secret_access_key=aws_config['secret_key'])
    
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = pd.read_json(obj['Body'])
    return data

def transform_data(data):
    # Convert UTC timestamps
    data['created_time'] = pd.to_datetime(data['created_time'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Remove non-UTF-8 characters
    string_columns = data.select_dtypes(include='object').columns
    for col in string_columns:
        data[col] = data[col].apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x)

    return data

def write_parquet_to_s3(data, bucket_name, key, aws_config):
    print('Writing file to S3......')
    # Convert DataFrame to Parquet
    parquet_file = BytesIO()
    data.to_parquet(parquet_file, index=False)
    parquet_file.seek(0)

    # Write Parquet to S3
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_config['access_key'],
                      aws_secret_access_key=aws_config['secret_key'])
    s3.put_object(Bucket=bucket_name, Key=key, Body=parquet_file)

def main():

    config = read_config()

    # AWS S3 Config
    aws_config = {
        'access_key': config['aws']['access_key'],
        'secret_key': config['aws']['secret_key'],
        'bucket_name':config['aws']['s3_bucket']
    }
    
    # Read data from S3
    bucket_name = aws_config['bucket_name']
    key = 'reddit_data.json'
    reddit_data = read_from_s3(bucket_name, key, aws_config)
    
    # Transform data
    cleaned_data = transform_data(reddit_data)
    
    # Write cleansed data to S3 as Parquet
    key = 'reddit_data_cleaned.parquet'
    write_parquet_to_s3(cleaned_data, bucket_name, key, aws_config)

if __name__ == "__main__":
    main()
