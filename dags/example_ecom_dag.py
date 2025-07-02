from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

CLICKHOUSE_JARS = "/opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.459.jar,/opt/bitnami/spark/jars/clickhouse-jdbc-0.6.0-all.jar"

with DAG(
    dag_id="ecom_processing_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["ecom", "spark", "example"],
) as dag:
    # wait_for_supplier_data = S3KeySensor(
    #     task_id="wait_for_supplier_data",
    #     bucket_name="company-datalake",
    #     bucket_key="raw/supplier_feeds/shop_a_suppliers/*.csv",
    #     aws_conn_id="minio_s3_conn", # Настроим это подключение в UI
    #     wildcard_match=True,
    #     timeout=180, # Ждать 3 минуты
    #     poke_interval=30, # Проверять каждые 30 секунд
    # )

    # Задача 2: Запустить Spark-джоб для обработки данных клиентов магазина А
    process_customers = SparkSubmitOperator(
        task_id="spark_process_customers",
        conn_id="spark_conn", # Настроим это подключение в UI
        application="/opt/airflow/spark_scripts/simple_spark_job.py",
        application_args=[
            "shop_a_schema.customers", # Аргумент 1: таблица для чтения
            "s3a://company-datalake/output/shop_a_customers_count" # Аргумент 2: путь для записи
        ],
        jars="/opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.459.jar",
        driver_class_path="/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
    )

    # # Задача 3: Запустить Spark-джоб для обработки данных клиентов магазина B
    # process_clients = SparkSubmitOperator(
    #     task_id="spark_process_clients",
    #     conn_id="spark_conn",
    #     application="/opt/airflow/spark_scripts/simple_spark_job.py",
    #     application_args=[
    #         "shop_b_schema.clients",
    #         "s3a://company-datalake/output/shop_b_clients_count"
    #     ],
    #     jars="/opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.459.jar",
    #     driver_class_path="/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
    # )
    
    # process_customers_click = SparkSubmitOperator(
    #     task_id="spark_process_customers",
    #     conn_id="spark_conn",
    #     application="/opt/airflow/spark_scripts/simple_spark_job.py",
    #     application_args=[
    #         "shop_a_schema.customers",
    #         "s3a://company-datalake/output/shop_a_customers_count",
    #         "shop_a_customers_count" # Аргумент 3: имя таблицы в ClickHouse
    #     ],
    #     jars=CLICKHOUSE_JARS,
    #     driver_class_path="/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
    # )

    # process_clients_click = SparkSubmitOperator(
    #     task_id="spark_process_clients",
    #     conn_id="spark_conn",
    #     application="/opt/airflow/spark_scripts/simple_spark_job.py",
    #     application_args=[
    #         "shop_b_schema.clients",
    #         "s3a://company-datalake/output/shop_b_clients_count",
    #         "shop_b_clients_count" # Аргумент 3: имя таблицы в ClickHouse
    #     ],
    #     jars=CLICKHOUSE_JARS,
    #     driver_class_path="/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
    # )

    # # Определяем последовательность выполнения задач
    # wait_for_supplier_data >> [process_customers, process_clients] # type: ignore
    # wait_for_supplier_data >> [process_customers_click, process_clients_click] # type: ignore

