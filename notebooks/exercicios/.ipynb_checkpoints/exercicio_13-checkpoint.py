# Exercício 13: Consultar apenas um particionamento
# Leia somente as vendas do ano 2023.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio13") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Consultar apenas vendas de 2023
result = spark.sql("SELECT * FROM lab.db.vendas WHERE ano = 2023")
result.show()

print("Consulta de partição específica (ano 2023) executada")

# Parar sessão
spark.stop()