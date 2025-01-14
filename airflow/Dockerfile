FROM apache/airflow:2.6.2-python3.9

USER root
# Instale dependências adicionais
RUN apt-get update && apt-get install -y \
    default-jre default-jdk vim nano \
    && rm -rf /var/lib/apt/lists/*
# Instale dependências do Python (ex: psycopg2, pymongo, etc.)

USER airflow

RUN pip install psycopg2-binary pymongo kafka-python pyspark

# Copie DAGs (se quiser em tempo de build, senão use volumes)
# COPY dags /opt/airflow/dags