from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from python_callable.utilities import JARS_FOR_SPARK, DRIVER_CLASS_PATH, SPARK_CONN_NAME

with DAG(
    dag_id="electroworld_overflow_from_pg_to_s3",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["ecom", "electroworld", "postgre", "s3", "datalake"],
) as dag:
    task_clients_to_s3 = SparkSubmitOperator(
        task_id="clients_to_s3",
        conn_id=SPARK_CONN_NAME,
        application="/opt/airflow/dags/spark_scripts/electoworld_overflow_from_pg_to_s3_spark_job.py",
        jars=JARS_FOR_SPARK,
        driver_class_path=DRIVER_CLASS_PATH
    )