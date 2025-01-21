from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.utils import AnalysisException
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

import json

def ingest_csv_to_kafka_topic():
    try:
        # Configurações do Kafka
        kafka_bootstrap_servers = "kafka:9092"
        topic_name = "meu_topico"

        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        
        # arquivo CSV
        df = pd.read_csv('/opt/airflow/spark-apps/data/top_universities.csv')
        
        # Enviar dados do CSV para o tópico Kafka
        for index, row in df.iterrows():
            message = {
                "University": row['University'],
                "Country": row['Country'],
                "City": row['City'],
                "Global_Rank": row['Global_Rank']
            }
            producer.send(topic_name, value=message)
        
        producer.flush()
        print("Dados enviados para o tópico Kafka com sucesso.")
    except Exception as e:
        print(f"Erro ao enviar dados para o Kafka: {e}")
    finally:
        producer.close()


def consume_kafka_to_postgres():
    try:
        # Configurações do Kafka
        kafka_bootstrap_servers = "kafka:9092"
        topic_name = "meu_topico"

        # Configurações do PostgreSQL
        db_config = {
            "host": "postgres",
            "port": 5432,
            "dbname": "airflow",
            "user": "airflow",
            "password": "airflow"
        }

        # Conexão com o PostgreSQL
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Criar tabela caso não exista
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_universities (
                university VARCHAR(255),
                country VARCHAR(255),
                city VARCHAR(255),
                global_rank INTEGER
            )
        """)
        conn.commit()

        # Consumir mensagens do Kafka
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id="consumer_group_1"
        )

        print(f"Conectado ao Kafka e consumindo dados do tópico '{topic_name}'...")

        for message in consumer:
            # Obter os dados da mensagem
            data = message.value

            # Inserir os dados no PostgreSQL
            cursor.execute("""
                INSERT INTO top_universities (university, country, city, global_rank)
                VALUES (%s, %s, %s, %s)
            """, (data['University'], data['Country'], data['City'], data['Global_Rank']))
            conn.commit()

            print(f"Dado inserido no PostgreSQL: {data}")

            if message.offset == consumer.end_offsets([TopicPartition(message.topic, message.partition)])[TopicPartition(message.topic, message.partition)] - 1:
                break

    except Exception as e:
        print(f"Erro ao consumir dados do Kafka ou salvar no PostgreSQL: {e}")
    finally:
        # Fechar conexões
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if consumer:
            consumer.close()
        print("Conexões encerradas.")


def migrate_postgresql_to_mongodb():
    try:
        # Conectar ao PostgreSQL
        pg_conn = psycopg2.connect(
            dbname="airflow", user="airflow", password="airflow", host="postgres", port="5432"
        )
        pg_cursor = pg_conn.cursor()
        
        # Selecionar dados da tabela PostgreSQL
        pg_cursor.execute("SELECT University, Country, City, Global_Rank FROM top_universities")
        rows = pg_cursor.fetchall()
        
        # Conectar ao MongoDB
        mongo_client = MongoClient('mongodb://admin:admin@mongo:27017/admin')
        mongo_db = mongo_client["mydb"]
        mongo_collection = mongo_db["top_universities"]
        
        # Inserir dados no MongoDB
        for row in rows:
            document = {
                "University": row[0],
                "Country": row[1],
                "City": row[2],
                "Global_Rank": row[3]
            }
            mongo_collection.insert_one(document)
        
        print("Migração de PostgreSQL para MongoDB concluída com sucesso.")
        mongo_client.close()
    
    except Exception as e:
        print(f"Erro durante a migração: {e}")
    
    finally:
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            pg_conn.close()
            pass
        if mongo_client:
            mongo_client.close()


def migrate_mongodb_to_postgresql():
    try:
        # Conectar ao MongoDB
        client = MongoClient('mongodb://admin:admin@mongo:27017/admin')
        db = client['mydb']
        collection = db['mycollection']

        # Conectar ao PostgreSQL
        conn = psycopg2.connect(
            dbname="airflow", user="airflow", password="airflow", host="postgres", port="5432"
        )
        cursor = conn.cursor()

        # Criar a tabela no PostgreSQL se não existir
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_universities_2 (
                University VARCHAR(255),
                Country VARCHAR(255),
                City VARCHAR(255),
                Global_Rank INTEGER
            )
        """)

        # Migrar dados do MongoDB para PostgreSQL
        for document in collection.find():
            cursor.execute(
                "INSERT INTO top_universities_2 (_id, University, Country, City, Global_Rank) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (_id) DO NOTHING",
            (str(document['_id']), document.get('University', None), document.get('Country', None), document.get('City', None), document.get('Global_Rank', None))
        )

        # Confirmar a transação
        conn.commit()
        print("Migração de MongoDB para PostgreSQL concluída com sucesso.")
    
    except Exception as e:
        print(f"Erro durante a migração: {e}")
    
    finally:
        # Fechar conexões
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if client:
            client.close()

def spark_streaming_kafka_to_postgresql():
    # Definindo o esquema dos dados
    schema = StructType([
        StructField("University", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Global_Rank", IntegerType(), True),
    ])

    try:
        # Criando a sessão Spark
        spark = SparkSession.builder \
            .appName("KafkaToPostgres") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .getOrCreate()

        # Configurações do Kafka
        kafka_bootstrap_servers = "kafka:9092"
        topic_name = "meu_topico"

        # Lendo dados do Kafka
        print("Lendo dados do Kafka...")
        df_kafka = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .load()
        print("Dados lidos do Kafka.")

        # Parsing dos dados e adicionando coluna de timestamp
        print("Parsing dos dados...")
        df_parsed = df_kafka \
            .selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), schema).alias("data")) \
            .select("data.University", "data.Country", "data.City", "data.Global_Rank") \
            .withColumn("timestamp", current_timestamp())
        print("Dados parseados.")

        # Filtrando os dados para incluir apenas o país "Brazil"
        print("Filtrando os dados para incluir apenas o país 'Brazil'...")
        df_filtered = df_parsed.filter(col("Country") == "Brazil")
        print("Dados filtrados.")

        # Adicionando marca d'água e agregando os dados
        print("Adicionando marca d'água e agregando os dados...")
        df_with_watermark = df_filtered.withWatermark("timestamp", "10 minutes")
        df_aggregated = df_with_watermark.groupBy("University", "Country", "City", "Global_Rank").count()
        print("Dados agregados.")

        # Ordenando os dados pelo campo "Global_Rank"
        print("Ordenando os dados pelo campo 'Global_Rank'...")
        df_sorted = df_aggregated.orderBy("Global_Rank")
        print("Dados ordenados.")

        # Função para processar cada micro-batch
        def foreach_batch_function(df, epoch_id):
            print(f"Processing batch {epoch_id} with {df.count()} records")
            conn = psycopg2.connect(
                dbname="airflow", user="airflow", password="airflow", host="postgres", port="5432"
            )
            cursor = conn.cursor()
            # Criar a tabela se não existir
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS university_rankings (
                    university VARCHAR(255),
                    country VARCHAR(255),
                    city VARCHAR(255),
                    global_rank INT
                );
            """)
            conn.commit()

            # Inserção em lote
            records = df.collect()
            insert_query = """
                INSERT INTO university_rankings (university, country, city, global_rank)
                VALUES (%s, %s, %s, %s)
            """
            cursor.executemany(insert_query, [(row["University"], row["Country"], row["City"], row["Global_Rank"]) for row in records])
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Batch {epoch_id} processed successfully")

        # Escrevendo os dados no PostgreSQL
        print("Iniciando a escrita dos dados no PostgreSQL...")
        query = df_sorted.writeStream \
            .outputMode("complete") \
            .foreachBatch(foreach_batch_function) \
            .start()

        # Aguarde um tempo limite específico para permitir que o stream seja processado
        query.awaitTermination(timeout=300)  # Aguarde 5 minutos (300 segundos)

        # Pare o stream após o tempo limite
        query.stop()

    except AnalysisException as e:
        print(f"Erro ao processar o stream: {e}")
    except ValueError as e:
        print(f"Erro de valor: {e}")
    except psycopg2.Error as e:
        print(f"Erro no PostgreSQL: {e}")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")


# Configuração padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Definição do DAG
with DAG(
    'spark_connect_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    ingest_csv_to_kafka_topic_task = PythonOperator(
        task_id='ingest_csv_to_kafka_topic',
        python_callable=ingest_csv_to_kafka_topic,  
    )

    consume_kafka_to_postgres_task = PythonOperator(
        task_id='consume_kafka_to_postgres',
        python_callable=consume_kafka_to_postgres,  
    )

    migrate_postgresql_to_mongodb_task = PythonOperator(
        task_id='migrate_postgresql_to_mongodb',
        python_callable=migrate_postgresql_to_mongodb
    )

    migrate_mongodb_to_postgresql_task = PythonOperator(
        task_id='migrate_mongodb_to_postgresql',
        python_callable=migrate_mongodb_to_postgresql
    )

    spark_streaming_kafka_to_postgresql_task = PythonOperator(
        task_id='spark_streaming_kafka_to_postgresql',
        python_callable=spark_streaming_kafka_to_postgresql,
    )

    ingest_csv_to_kafka_topic_task >> consume_kafka_to_postgres_task >> migrate_postgresql_to_mongodb_task >> migrate_mongodb_to_postgresql_task >> spark_streaming_kafka_to_postgresql_task