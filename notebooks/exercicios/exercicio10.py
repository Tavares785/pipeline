# Exercício 10: Deletar um registro
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio10") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

spark.sql("DELETE FROM lab.db.pessoas WHERE nome = 'Bob'")
spark.sql("SELECT * FROM lab.db.pessoas").show()