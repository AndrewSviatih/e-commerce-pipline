# Используем официальный образ Airflow 2.9.2, который работает на Python 3.11
FROM apache/airflow:2.9.2-python3.11

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt /requirements.txt

# Устанавливаем Airflow с провайдерами (включая clickhouse) и наши доп. библиотеки
RUN pip install --no-cache-dir \
    "apache-airflow[postgres,apache.spark,amazon,clickhouse]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" \
    && pip install --no-cache-dir -r /requirements.txt