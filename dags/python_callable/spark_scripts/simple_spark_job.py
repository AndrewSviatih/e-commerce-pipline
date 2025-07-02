import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    postgres_table = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName(f"Processing {postgres_table}").getOrCreate()

    # 1. Чтение из PostgreSQL
    pg_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/ecom_db") \
        .option("dbtable", postgres_table) \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    print(f"Прочитано {pg_df.count()} строк из таблицы {postgres_table}.")

    # 2. Простая трансформация (подсчет строк)
    count_df = pg_df.summary("count")

    # 3. Запись результата в S3 (MinIO)
    # repartition(1) чтобы получить один выходной файл
    count_df.repartition(1).write.mode("overwrite").format("json").save(output_path)

    print(f"Результат подсчета записан в {output_path}")

    spark.stop()