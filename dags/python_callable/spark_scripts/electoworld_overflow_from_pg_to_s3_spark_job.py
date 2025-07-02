import sys
from pyspark.sql import SparkSession, DataFrame

def get_df_from_table_postgres(spark: SparkSession, schema_nm: str, table_nm: str) -> DataFrame:
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/ecom_db") \
        .option("dbtable", table_nm) \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    return df
        

if __name__ == "__main__":
    schema_nm = 'electroworld'
    table_nm =  'clients'

    spark = SparkSession.builder.appName(f"Processing {table_nm}").getOrCreate()

    items_df = get_df_from_table_postgres(spark, schema_nm, table_nm)

    items_df.show()

    # # 3. Запись результата в S3 (MinIO)
    # # repartition(1) чтобы получить один выходной файл
    # count_df.repartition(1).write.mode("overwrite").format("json").save(output_path)

    # print(f"Результат подсчета записан в {output_path}")

    spark.stop()