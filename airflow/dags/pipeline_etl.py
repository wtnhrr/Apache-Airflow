from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='pipeline_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['etl']
) as dag:

    # 1) Ingestão de Arquivos (CSV/JSON) -> Postgres
    ingest_files = BashOperator(
        task_id='ingest_files',
        bash_command="""
        /home/airflow/.local/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
          /opt/spark-apps/file_ingestion.py
        """
    )

    # 2) Migração Postgres -> Mongo
    postgres_to_mongo = BashOperator(
        task_id='postgres_to_mongo',
        bash_command="""
        /home/airflow/.local/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
          /opt/spark-apps/postgres_to_mongo.py
        """
    )

    # 3) Migração Mongo -> Postgres
    mongo_to_postgres = BashOperator(
        task_id='mongo_to_postgres',
        bash_command="""
        /home/airflow/.local/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
          /opt/spark-apps/mongo_to_postgres.py
        """
    )

    # 4) Spark Streaming (Kafka -> Postgres) 
    run_streaming = BashOperator(
        task_id='run_streaming',
        bash_command="""
        /home/airflow/.local/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
          /opt/spark-apps/streaming_app.py
        """
    )

    # 5) Finalização
    finalize = BashOperator(
        task_id='finalize',
        bash_command='echo "ETL COMPLETO!"'
    )

    # ordem do pipeline
    ingest_files >> postgres_to_mongo >> mongo_to_postgres >> run_streaming >> finalize