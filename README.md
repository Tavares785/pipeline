# Exercícios de Big Data

⚙️ Requisitos

Docker e Docker Compose

Python 3.10+ (caso rode localmente)

Apache Spark configurado (no container lab ou via docker-compose)

🚀 Como Rodar Localmente

Clone o repositório

git clone https://github.com/seu-usuario/pipeline.git
cd pipeline


Suba o ambiente do laboratório

docker compose -f docker/docker-compose.grader.yml up -d


Acesse o container

docker exec -it lab bash


Abra o Jupyter Notebook (se aplicável)

http://localhost:8888

🧩 Rodando os Exercícios Manualmente

Dentro do container ou notebook:

from pyspark.sql import SparkSession
from exercicios import *

spark = SparkSession.builder.getOrCreate()

# Exemplo: criar e visualizar DataFrame
df = ex01_create_df(spark)
df.show()

# Exemplo: salvar CSV no HDFS
ex02_save_csv(spark, "hdfs://namenode:9000/data/ex1.csv")

🧪 Executando os Testes
Testes Unitários:
pytest tests/unit -v

Testes de Integração (requer HDFS, Iceberg e Metastore ativos):
pytest tests/integration -v

Rodar todos os testes:
pytest -v

⚙️ Pipeline de Avaliação (GitHub Actions)

O arquivo .github/workflows/grade.yml executa automaticamente:

Build do ambiente Docker (Dockerfile.grader)

Instalação das dependências

Execução dos testes (pytest)

Geração da nota e feedback

📦 Dependências Principais (requirements.txt)
pyspark
pytest
pyiceberg


O Spark e o Hive Metastore são configurados via Docker Compose.

💡 Dicas Úteis

Verifique logs do Hive Metastore:

docker logs hive-metastore


Verifique diretórios no HDFS:

hdfs dfs -ls /


Verifique tabelas Iceberg:

spark.sql("SHOW TABLES IN lab.db").show()

👨‍💻 Autor

Seu Nome
💼 | 🎓 Estudante de ADS
🚀 Foco em Big Data, Spark e Arquiteturas Distribuídas