import sys
from pyspark.sql import SparkSession, DataFrame,  types
from pyspark.sql import functions as f
from python_callable.utilities import get_df_from_table_postgres


def get_df_cdc_from_changes_postgre(df: DataFrame, schema_nm: str, table_nm: str, last_get_of_changes: types.TimestampType) -> DataFrame:
    df_changes = df \
        .select('')

if __name__ == "__main__":
    schema_nm = 'public'
    table_nm = 'change_log'
    
    # Include PostgreSQL JDBC driver via Maven coordinate
    spark = SparkSession.builder \
        .appName(f"Processing {table_nm}") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()
    
    changes_log = get_df_from_table_postgres(spark, schema_nm, table_nm)
    changes_log.show()

    
    
    spark.stop()