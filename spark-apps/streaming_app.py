from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.utils import AnalysisException

schema = StructType([
    StructField("University", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Global Rank", IntegerType(), True),
])

if __name__ == "__main__":
    try:
        spark = SparkSession.builder \
            .appName("KafkaStreaming") \
            .getOrCreate()

        kafka_bootstrap_servers = "kafka:9092"
        topic_name = "meu_topico"

        df_kafka = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .load()

        df_parsed = df_kafka \
            .selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), schema).alias("data")) \
            .select("data.University", "data.Country", "data.City", "data.Global Rank")

        query = df_parsed.writeStream \
            .foreachBatch(lambda df, epochId: df.write
                          .format("jdbc")
                          .option("url", "jdbc:postgresql://postgres:5432/airflow")
                          .option("dbtable", "public.streaming_results")
                          .option("user", "airflow")
                          .option("password", "airflow")
                          .mode("append")
                          .save()) \
            .outputMode("append") \
            .start()

        query.awaitTermination()

    except AnalysisException as e:
        print(f"Error processing stream: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")