from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from configparser import ConfigParser
from reddit_extract import main as fetch_and_write_reddit_data
from transform import main as transform_and_write_reddit_data
from aggregation import main as calculate_aggregations
from dags.db_load import main as create_mysql_table

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 29),
    'retries': 1,
}

with DAG('reddit_etl_pipeline', default_args=default_args, schedule_interval=None) as dag:
    fetch_reddit_data = PythonOperator(
        task_id='fetch_reddit_data',
        python_callable=fetch_and_write_reddit_data
    )

    transform_and_write_data = PythonOperator(
        task_id='transform_and_write_data',
        python_callable=transform_and_write_reddit_data
    )

    aggregate = PythonOperator(
        task_id='aggregate_cleaned_data',
        python_callable=calculate_aggregations
    )

    db_load = PythonOperator(
        task_id='load_agg_data_mysql',
        python_callable= create_mysql_table
    )


fetch_reddit_data >> transform_and_write_data >> aggregate >> db_load
