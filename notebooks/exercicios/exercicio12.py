# Exercício 12: Inserir dados particionados
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio12") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

spark.sql("INSERT INTO lab.db.vendas VALUES (1, 1000.50, 2022)")
spark.sql("INSERT INTO lab.db.vendas VALUES (2, 2500.75, 2023)")
spark.sql("INSERT INTO lab.db.vendas VALUES (3, 1800.25, 2023)")
spark.sql("INSERT INTO lab.db.vendas VALUES (4, 3200.00, 2024)")