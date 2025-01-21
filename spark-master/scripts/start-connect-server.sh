#!/bin/bash

# Instalar pacotes adicionais
pip install pyarrow grpcio grpcio-status protobuf

# Iniciar o Spark Connect
start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.4

# Inicie o Spark Master
/opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master --host spark-master --port 7077 --webui-port 8080