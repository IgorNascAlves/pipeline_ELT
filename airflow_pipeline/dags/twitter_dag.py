import sys
sys.path.append("airflow_pipeline")

from os.path import join
from pathlib import Path

from airflow.models import DAG
from airflow.utils.dates import days_ago

from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6),
}
BASE_FOLDER = join(
    str(Path("~/Documents").expanduser()),
    "pipeline_ELT/datalake/{stage}/twitter_nbabrasil/{partition}",
)
PARTITION_FOLDER_EXTRACT = "extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

with DAG(
    dag_id="twitter_dag",
    default_args=ARGS,
    schedule_interval = "@daily",
    max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="NBABrasil",
        file_path=join(BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT), "AluraOnline_{{ ds_nodash }}.json"),
        start_time = "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        end_time = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}"
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        application=join(Path(__file__).parents[1], "spark/transformation.py"),
        name="twitter_transformation",
        application_args=["--src", BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),
                          "--dest", BASE_FOLDER.format(stage="Silver", partition=""),
                          "--process-date", "{{ ds }}"
                         ]
    )

    twitter_insight = SparkSubmitOperator(
        task_id="insight_twitter",
        application=join(Path(__file__).parents[1], "spark/insight_tweet.py"),
        name="twitter_insight",
        application_args=["--src", BASE_FOLDER.format(stage="Silver", partition=""),
                          "--dest", BASE_FOLDER.format(stage="Gold", partition=""),
                          "--process-date", "{{ ds }}",
                          "--user-id", "589433710"
                         ]
    )

twitter_operator >> twitter_transform >> twitter_insight