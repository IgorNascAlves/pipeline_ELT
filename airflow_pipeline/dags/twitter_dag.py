import sys
sys.path.append("airflow_pipeline")

from os.path import join

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.macros import ds_add

from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6),
}

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

with DAG(
    dag_id="twitter_dag",
    default_args=ARGS,
    schedule_interval = "@daily",
    max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path=join("datalake/twitter_nbabrasil", "extract_date={{ ds }}", "AluraOnline_{{ ds_nodash }}.json"),
        start_time = "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        end_time = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}"
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        application="/home/alura/Documents/pipeline_ELT/pipeline_ELT/airflow_pipeline/spark/transformation.py",
        name="twitter_transformation",
        application_args=[
            "--src",
            join("datalake/twitter_nbabrasil", "extract_date={{ ds }}"),
            "--dest",
            join("datalake/twitter_nbabrasil","silver"),
            "--process-date",
            "{{ ds }}",
        ]
    )

twitter_operator >> twitter_transform