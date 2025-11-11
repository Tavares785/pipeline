# Exercício 14: Ver metadados da tabela
# Use DESCRIBE HISTORY e DESCRIBE DETAIL.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio14") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Ver histórico da tabela
print("=== HISTÓRICO DA TABELA ===")
history = spark.sql("DESCRIBE HISTORY lab.db.vendas")
history.show()

# Ver detalhes da tabela
print("=== DETALHES DA TABELA ===")
detail = spark.sql("DESCRIBE DETAIL lab.db.vendas")
detail.show()

# Parar sessão
spark.stop()