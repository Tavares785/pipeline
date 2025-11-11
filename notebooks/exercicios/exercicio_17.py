# Exercício 17: Leitura incremental (Time Travel)
# Volte para uma versão anterior da tabela.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio17") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Consultar versão atual
print("=== VERSÃO ATUAL ===")
current = spark.sql("SELECT * FROM lab.db.vendas")
current.show()

# Consultar versão anterior (snapshot 1)
print("=== VERSÃO ANTERIOR (SNAPSHOT 1) ===")
previous = spark.sql("SELECT * FROM lab.db.vendas VERSION AS OF 1")
previous.show()

# Parar sessão
spark.stop()