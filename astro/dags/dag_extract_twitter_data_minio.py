from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from os.path import join
from airflow.utils.dates import days_ago
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# Variables
timestamp_format = "%Y-%m-%dT%H:%M:%S.00Z"
query="AluraOnline"

with DAG(
    dag_id="dag_extract_twitter_data_minio",
    start_date=days_ago(6),
    schedule_interval="@daily"
) as dag:

    extract_twitter_data = TwitterOperator(
        task_id="extract_twitter_data",
        query=query,
        file_path=join(
            "datalake/bronze/twitter_alura",
            "extract_data_{{ ds_nodash }}",
            "alura_{{ ds_nodash }}.json"
        ),
        start_time=(
            "{{"
            f"data_interval_start.strftime('{ timestamp_format }')"
            "}}"
        ),
        end_time=(
            "{{"
            f"data_interval_end.strftime('{ timestamp_format }')"
            "}}"
        ),
    )

    transform_twitter_data = SparkSubmitOperator(
        task_id="transform_twitter_data",
        application=(
            "spark/transformation.py"
        ),
        name="twitter_transformation",
        application_args=[
            "--src",
            "datalake/bronze/twitter_alura/extract_data_{{ ds_nodash }}",
            "--dest",
            "datalake/silver/twitter_alura",
            "--process-date",
            "{{ ds_nodash }}"
        ]
    )

    extract_twitter_data >> transform_twitter_data
