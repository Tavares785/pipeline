# Exercício 4: Criar namespace Iceberg
# Crie um namespace chamado lab.db no catálogo Iceberg.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio4") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Criar namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.db")

print("Namespace lab.db criado com sucesso")

# Parar sessão
spark.stop()