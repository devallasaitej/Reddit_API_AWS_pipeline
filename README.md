# Reddit API - AWS - Python - Airflow Data Pipeline
## Overview
The objective of this project is to orchestarate a data pipeline using Airflow which runs in docker to acquire data from a subreddit - r/dataengineering using reddit's API, cleanse acquired data and finally load reporting level data to Amazon RDS MySQL table.
## Platforms Used
1. Airflow: Workflow orchestration management platform
2. AWS S3: Object storage service to store raw, cleansed and aggregated formats of data
3. AWS RDS: Relational data service to store final aggregated - reporting layer data in a table
4. AWS IAM: Identity and Access management service to create roles to access AWS S3
## Data Pipeline Architecture
<img width="816" alt="Reddit_datapipeline" src="https://github.com/devallasaitej/Reddit_API_AWS_pipeline/assets/64268620/ec21b250-b156-4a7b-b47d-ef00d087cdcb">

## Airflow DAG
<img width="816" alt="Screenshot 2024-03-02 at 12 23 57 PM" src="https://github.com/devallasaitej/Reddit_API_AWS_pipeline/assets/64268620/f37e4d06-8fab-47b5-a166-d27bfd7a3098">


## Final RDS Table snapshot
<img width="816" alt="image" src="https://github.com/devallasaitej/Reddit_API_AWS_pipeline/assets/64268620/7d2861b2-4821-4352-bacd-83d0ae8304d5">
