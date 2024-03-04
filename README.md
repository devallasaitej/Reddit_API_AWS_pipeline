# Reddit API - AWS - Python - Airflow Data Pipeline
## Overview
The objective of this project is to orchestarate a data pipeline using Airflow which runs in docker to acquire data from a subreddit - r/dataengineering using reddit's API, cleanse acquired data and finally load reporting level data to Amazon RDS MySQL table.
## Platforms Used
1. Airflow: Workflow orchestration management platform
2. AWS S3: Object storage service to store raw, cleansed and aggregated formats of data
3. AWS RDS: Relational data service to store final aggregated - reporting layer data in a table
4. AWS IAM: Identity and Access management service to create roles to access AWS S3
## Data Pipeline Architecture
<img width="816" alt="Reddit_datapipeline" src="https://github.com/devallasaitej/Reddit_API_AWS_pipeline/assets/64268620/fdd383da-60c6-4b45-9c2c-f3f3b4053764">


## Airflow DAG
<img width="816" alt="Screenshot 2024-03-02 at 12 23 57 PM" src="https://github.com/devallasaitej/Reddit_API_AWS_pipeline/assets/64268620/15ce5c5c-a39a-46d7-95b4-ddf28ce659bc">



## Final RDS Table snapshot
<img width="816" alt="image" src="https://github.com/devallasaitej/Reddit_API_AWS_pipeline/assets/64268620/b6a726b3-a8d2-43c8-8d4a-aab1e7e1c174">

