# Exercício 17: Leitura incremental (Time Travel)
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio17") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

spark.sql("SELECT * FROM lab.db.vendas").show()
spark.sql("SELECT * FROM lab.db.vendas VERSION AS OF 1").show()