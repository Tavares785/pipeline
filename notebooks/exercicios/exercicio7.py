# Exercício 7: Ler tabela Iceberg
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio7") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

result = spark.sql("SELECT * FROM lab.db.pessoas")
result.show()