# Exercício 6: Inserir dados na tabela Iceberg
# Insira 3 valores manualmente usando SQL INSERT INTO.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio6") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Inserir dados na tabela
spark.sql("INSERT INTO lab.db.pessoas VALUES (1, 'Alice')")
spark.sql("INSERT INTO lab.db.pessoas VALUES (2, 'Bob')")
spark.sql("INSERT INTO lab.db.pessoas VALUES (3, 'Carlos')")

print("Dados inseridos na tabela lab.db.pessoas")

# Parar sessão
spark.stop()