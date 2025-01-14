from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

if __name__ == "__main__":
    try:
        spark = SparkSession.builder \
            .appName("FileIngestion") \
            .getOrCreate()

        df_csv = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("/opt/spark-apps/data/*.csv")

        df_json = spark.read \
            .format("json") \
            .option("inferSchema", "true") \
            .load("/opt/spark-apps/data/*.json")

        df_csv_selected = df_csv.select("University", "Country", "City", "Global Rank")

        df_json_selected = df_json.select("University", "Country", "City", "Global Rank")

        df_union = df_csv_selected.union(df_json_selected)

        df_union.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "public.files_ingested") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .mode("append") \
            .save()

    except AnalysisException as e:
        print(f"Error reading files: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")