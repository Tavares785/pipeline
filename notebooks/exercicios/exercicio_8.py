# Exercício 8: Contar registros
# Conte quantos registros existem na tabela lab.db.pessoas.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio8") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Contar registros
count = spark.sql("SELECT COUNT(*) as total FROM lab.db.pessoas")
count.show()

# Parar sessão
spark.stop()