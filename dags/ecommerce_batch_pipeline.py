from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "suraj",
    "start_date": datetime(2026, 3, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False
}

with DAG(
    dag_id="ecommerce_batch_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Ecommerce Multi-Source Batch Data Pipeline"
) as dag:

    extract_csv = BashOperator(
        task_id="extract_csv",
        bash_command="docker exec ecommerce_spark python3 /app/jobs/extraction/extract_csv.py"
    )

    extract_db = BashOperator(
        task_id="extract_db",
        bash_command="docker exec ecommerce_spark python3 /app/jobs/extraction/extract_db.py"
    )

    extract_api = BashOperator(
        task_id="extract_api",
        bash_command="docker exec ecommerce_spark python3 /app/jobs/extraction/extract_api.py"
    )

    raw_to_staging = BashOperator(
        task_id="raw_to_staging",
        bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/jobs/transformation/raw_to_staging.py"
    )

    validate_data = BashOperator(
        task_id="validate_data",
        bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/jobs/validation/validate_data.py"
    )

    staging_to_processed = BashOperator(
        task_id="staging_to_processed",
        bash_command="docker exec ecommerce_spark /opt/spark/bin/spark-submit /app/jobs/transformation/staging_to_processed.py"
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command="""
        docker exec ecommerce_spark /opt/spark/bin/spark-submit \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --packages org.postgresql:postgresql:42.7.3 \
        /app/jobs/warehouse/load_to_postgres.py
        """
    )

    [extract_csv, extract_db, extract_api] >> raw_to_staging >> validate_data >> staging_to_processed >> load_to_postgres