📌 Descrição do Projeto

Este projeto tem como objetivo desenvolver um pipeline de processamento de dados em tempo real utilizando Apache Spark Streaming e Apache Airflow.

A aplicação será responsável por consumir dados em tempo real a partir de um Data Lake, que armazenará dados estruturados e semi-estruturados, como:

✅ Bancos de dados relacionais (PostgreSQL)
✅ Bancos de dados NoSQL (MongoDB)
✅ Arquivos JSON e CSV no sistema de arquivos local

A ingestão e entrega de dados serão realizadas com Apache Kafka e Debezium, garantindo alta performance e baixa latência na transmissão dos dados.

🎯 Objetivos

Criar um Data Lake para armazenar dados estruturados e semi-estruturados
Desenvolver aplicações PySpark para consumir e transformar os dados
Implementar Apache Kafka/Debezium para ingestão e entrega dos dados
Utilizar Apache Airflow para orquestração e monitoramento dos fluxos de ETL

🔧 Tecnologias Utilizadas

Apache Spark	Processamento distribuído de dados
Apache Airflow	Orquestração e monitoramento de ETL
Apache Kafka	Streaming de eventos em tempo real
Debezium	Captura de mudanças em bancos de dados
PostgreSQL	Banco de dados relacional
MongoDB	Banco de dados NoSQL
PySpark	Biblioteca para trabalhar com Spark em Python

🏗️ Arquitetura do Projeto
A arquitetura sugerida para este projeto segue o fluxo abaixo:

1️⃣ Os dados são armazenados no Data Lake (PostgreSQL, MongoDB, JSON, CSV).
2️⃣ O Apache Kafka/Debezium realiza a ingestão e entrega dos dados em tempo real.
3️⃣ O Apache Spark Streaming (PySpark) processa e transforma os dados consumidos.
4️⃣ O Apache Airflow agenda, orquestra e monitora os pipelines de ETL.
