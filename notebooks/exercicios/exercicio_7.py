# Exercício 7: Ler tabela Iceberg
# Faça uma query SELECT * FROM lab.db.pessoas e exiba o resultado.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio7") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Ler tabela Iceberg
result = spark.sql("SELECT * FROM lab.db.pessoas")
result.show()

# Parar sessão
spark.stop()