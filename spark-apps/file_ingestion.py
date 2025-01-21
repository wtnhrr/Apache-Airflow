from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import logging
import os

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        spark = SparkSession.builder \
            .appName("FileIngestion") \
            .config("spark.sql.shuffle.partitions", "50") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()

        df_csv = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("spark-apps/data/*.csv")

        df_json = spark.read \
            .format("json") \
            .option("inferSchema", "true") \
            .load("spark-apps/data/*.json")

        # Validação de dados
        if df_csv is None or df_json is None:
            logger.error("Erro ao ler os arquivos CSV ou JSON")
            return

        df_csv_selected = df_csv.select("University", "Country", "City", "Global Rank")
        df_json_selected = df_json.select("University", "Country", "City", "Global Rank")

        df_union = df_csv_selected.union(df_json_selected)

        # Parâmetros de conexão
        jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/airflow")
        jdbc_table = os.getenv("JDBC_TABLE", "public.files_ingested")
        jdbc_user = os.getenv("JDBC_USER", "airflow")
        jdbc_password = os.getenv("JDBC_PASSWORD", "airflow")

        df_union.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", jdbc_table) \
            .option("user", jdbc_user) \
            .option("password", jdbc_password) \
            .mode("append") \
            .save()

        logger.info("Ingestão de arquivos concluída com sucesso")

    except AnalysisException as e:
        logger.error(f"Erro ao ler os arquivos: {e}")
    except Exception as e:
        logger.error(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    main()