import pandas as pd
import pymysql
import boto3
from sqlalchemy import create_engine
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
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_config['access_key'],
                      aws_secret_access_key=aws_config['secret_key'])
    
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = pd.read_parquet(BytesIO(obj['Body'].read()))
    return data

def create_mysql_table(engine):
    # Define the DDL statement for creating the MySQL table
    ddl_statement = """
    CREATE TABLE IF NOT EXISTS reddit_posts_agg(
        flair VARCHAR(255),
        hour INT,
        day INT,
        month INT,
        year INT,
        title INT,
        num_comments INT,
        score INT,
        award_count INT
    );
    """
    # Execute the DDL statement
    with engine.connect() as connection:
        connection.execute(ddl_statement)
    print("MySQL table created successfully")

def main():

    config = read_config()

    # MySQL Config
    mysql_config = {
        'host': config['mysql']['host'],
        'user': config['mysql']['user'],
        'password': config['mysql']['password'],
        'database': config['mysql']['database']
    }

    # AWS S3 Config
    aws_config = {
        'access_key': config['aws']['access_key'],
        'secret_key': config['aws']['secret_key'],
        'bucket_name':config['aws']['s3_bucket']
    }
        
    # Read cleaned data from S3
    bucket_name = aws_config['bucket_name']
    key = 'reddit_data_aggregated.parquet'
    aggregated_data = read_from_s3(bucket_name, key, aws_config)

    # Connect to MySQL
    engine = create_engine(f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}/{mysql_config['database']}")

    # Create MySQL table
    create_mysql_table(engine)

    # Write DataFrame to MySQL table
    table_name = 'reddit_posts_agg'
    aggregated_data.to_sql(table_name, con=engine, if_exists='append', index=False)

    print("Data successfully written to MySQL table")

if __name__ == "__main__":
    main()
