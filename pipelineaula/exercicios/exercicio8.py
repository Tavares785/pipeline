# Exercício 8: Contar registros
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio8") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

count_result = spark.sql("SELECT COUNT(*) as total FROM lab.db.pessoas")
count_result.show()