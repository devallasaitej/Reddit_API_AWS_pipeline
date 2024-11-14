import praw
import boto3
import json
import os
from datetime import datetime
from configparser import ConfigParser

# Get the directory path of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to config.json
config_file_path = os.path.join(current_dir, 'config.json')

def read_config(filename=config_file_path):
    print('Reading config.json ....')
    with open(filename, 'r') as f:
        return json.load(f)

config = read_config()

def fetch_data_from_reddit(subreddit_name, reddit_config):
    print('Executing fetch data function ....')
    reddit = praw.Reddit(client_id=reddit_config['client_id'],
                         client_secret=reddit_config['client_secret'],
                         user_agent=reddit_config['user_agent'])

    print('current_directory:',current_dir)
    print('config_file_path:',config_file_path)
    
    subreddit = reddit.subreddit(subreddit_name)
    posts = []

    for submission in subreddit.top(limit=20000):  # Adjust limit as needed
        post_data = {
            'title': submission.title,
            'author': submission.author.name if submission.author else '[deleted]',
            'score': submission.score,
            'num_comments': submission.num_comments,
            'url': submission.url,
            'created_time': datetime.fromtimestamp(submission.created_utc).isoformat(),
            'subreddit': submission.subreddit.display_name,
            'post_id': submission.id,
            'flair': submission.link_flair_text if submission.link_flair_text else None,
            'upvote_ratio': submission.upvote_ratio,
            'award_count': len(submission.all_awardings)
        }
        posts.append(post_data)

    return posts

def write_to_s3(data, bucket_name, key, aws_config):
    print('Writing file to S3.....')
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_config['access_key'],
                      aws_secret_access_key=aws_config['secret_key'])

    s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))

def main():
    print('Entered main function ...')
    # Read config
    config = read_config()

    # Reddit API Config
    reddit_config = {
        'client_id': config['reddit']['client_id'],
        'client_secret': config['reddit']['client_secret'],
        'user_agent': config['reddit']['user_agent']
    }

    # AWS S3 Config
    aws_config = {
        'access_key': config['aws']['access_key'],
        'secret_key': config['aws']['secret_key']
    }

    # Fetch data from Reddit
    subreddit_name = 'dataengineering'
    reddit_data = fetch_data_from_reddit(subreddit_name, reddit_config)

    # Write data to S3
    bucket_name = config['aws']['s3_bucket']
    key = 'reddit_data.json'
    write_to_s3(reddit_data, bucket_name, key, aws_config)

if __name__ == "__main__":
    main()
