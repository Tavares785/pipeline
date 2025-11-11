# Exercício 14: Ver metadados da tabela
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio14") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

spark.sql("DESCRIBE HISTORY lab.db.vendas").show(truncate=False)
spark.sql("DESCRIBE DETAIL lab.db.vendas").show(truncate=False)