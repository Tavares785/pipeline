# Exercício 13: Consultar apenas um particionamento
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio13") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

vendas_2023 = spark.sql("SELECT * FROM lab.db.vendas WHERE ano = 2023")
vendas_2023.show()