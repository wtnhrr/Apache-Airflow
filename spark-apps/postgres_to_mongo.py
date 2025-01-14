from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PostgresToMongo") \
        .getOrCreate()

    # Exemplo: Ler do Postgres via JDBC
    df_pg = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "public.mytable") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .load()

    # Exemplo: Escrever no Mongo (é preciso ter o pacote "org.mongodb.spark:mongo-spark-connector")
    df_pg.write \
        .format("mongodb") \
        .option("uri", "mongodb://admin:admin@mongo:27017") \
        .option("database", "mydb") \
        .option("collection", "mycollection_migrated") \
        .mode("append") \
        .save()

    spark.stop()
