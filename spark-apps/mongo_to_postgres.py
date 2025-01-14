from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MongoToPostgres") \
        .getOrCreate()

    # Ler do Mongo
    df_mongo = spark.read \
        .format("mongodb") \
        .option("uri", "mongodb://admin:admin@mongo:27017/mydb.mycollection") \
        .load()

    # Escrever em Postgres
    df_mongo.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "public.mytable_migrated") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("append") \
        .save()

    spark.stop()