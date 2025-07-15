from pyspark.sql import SparkSession, DataFrame

JARS_FOR_SPARK = "/opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.459.jar"
DRIVER_CLASS_PATH = "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"

SPARK_CONN_NAME = 'spark_conn'

def get_df_from_table_postgres(spark: SparkSession, schema, table) -> DataFrame:
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/ecom_db") \
        .option("dbtable", f"{schema}.{table}")  \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df