#!/bin/bash

echo "Iniciando ambiente Big Data..."

# Subir todos os serviços
docker-compose up -d

echo "Aguardando serviços iniciarem..."
sleep 60

# Criar diretórios necessários no HDFS
echo "Criando diretórios no HDFS..."
docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -mkdir -p /export
docker exec namenode hdfs dfs -chmod 777 /data
docker exec namenode hdfs dfs -chmod 777 /export

# Configurar Superset
echo "Configurando Superset..."
docker exec superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@superset.com --password admin
docker exec superset superset db upgrade
docker exec superset superset init

echo "Ambiente pronto!"
echo "Jupyter: http://localhost:8888"
echo "Spark UI: http://localhost:8080"
echo "HDFS: http://localhost:9870"
echo "Trino: http://localhost:8082"
echo "Superset: http://localhost:8088 (admin/admin)"