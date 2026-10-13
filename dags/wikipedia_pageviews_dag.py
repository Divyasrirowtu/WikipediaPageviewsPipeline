
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="wikipedia_pageviews_pipeline",
    default_args=default_args,
    description="Ingest, transform, aggregate, and validate Wikimedia pageviews",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["wikipedia", "pageviews", "data-engineering"],
) as dag:

    ingest_pageviews = BashOperator(
        task_id="ingest_pageviews",
        bash_command="python /opt/airflow/scripts/ingest_pageviews.py",
    )

    transform_pageviews = BashOperator(
        task_id="transform_pageviews",
        bash_command="python /opt/airflow/scripts/transform_pageviews.py",
    )

    aggregate_pageviews = BashOperator(
        task_id="aggregate_pageviews",
        bash_command="python /opt/airflow/scripts/aggregate_pageviews.py",
    )

    validate_pipeline = BashOperator(
        task_id="validate_pipeline",
        bash_command="python /opt/airflow/scripts/validate_pipeline.py",
    )

    (
        ingest_pageviews
        >> transform_pageviews
        >> aggregate_pageviews
        >> validate_pipeline
    )
