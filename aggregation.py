import pandas as pd
import boto3
from io import BytesIO
import os
import json

# Get the directory path of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to config.json
config_file_path = os.path.join(current_dir, 'config.json')

def read_config(filename=config_file_path):
    print('Reading config.json ....')
    with open(filename, 'r') as f:
        return json.load(f)
    
def read_from_s3(bucket_name, key, aws_config):
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_config['access_key'],
                      aws_secret_access_key=aws_config['secret_key'])
    
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = pd.read_parquet(BytesIO(obj['Body'].read()))
    return data

def write_parquet_to_s3(data, bucket_name, key, aws_config):
    # Convert DataFrame to Parquet
    parquet_file = BytesIO()
    data.to_parquet(parquet_file, index=False)
    parquet_file.seek(0)

    # Write Parquet to S3
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_config['access_key'],
                      aws_secret_access_key=aws_config['secret_key'])
    s3.put_object(Bucket=bucket_name, Key=key, Body=parquet_file)

def calculate_aggregations(data):

    # Convert created_time to datetime format
    data['created_time'] = pd.to_datetime(data['created_time'])
    
    # Extract hour, day, month, and year from created_time
    data['hour'] = data['created_time'].dt.hour
    data['day'] = data['created_time'].dt.day
    data['month'] = data['created_time'].dt.month
    data['year'] = data['created_time'].dt.year
    
    # Perform aggregations
    aggregations = data.groupby(['flair', 'hour', 'day', 'month', 'year']).agg({
        'title': 'count',
        'num_comments': 'sum',
        'score': 'sum',
        'award_count': 'sum'
    }).reset_index()
    
    return aggregations

def main():
    config = read_config()

    # AWS S3 Config
    aws_config = {
        'access_key': config['aws']['access_key'],
        'secret_key': config['aws']['secret_key'],
        'bucket_name':config['aws']['s3_bucket']
    }
        
    # Read cleaned data from S3
    bucket_name = aws_config['bucket_name']
    key = 'reddit_data_cleaned.parquet'
    cleaned_data = read_from_s3(bucket_name, key, aws_config)
    
    # Perform aggregations
    aggregated_data = calculate_aggregations(cleaned_data)
    
    # Write aggregated data to S3 as Parquet
    key = 'reddit_data_aggregated.parquet'
    write_parquet_to_s3(aggregated_data, bucket_name, key, aws_config)

if __name__ == "__main__":
    main()
