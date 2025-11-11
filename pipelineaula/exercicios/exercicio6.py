# Exercício 6: Inserir dados na tabela Iceberg
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio6") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

spark.sql("INSERT INTO lab.db.pessoas VALUES (1, 'Alice')")
spark.sql("INSERT INTO lab.db.pessoas VALUES (2, 'Bob')")
spark.sql("INSERT INTO lab.db.pessoas VALUES (3, 'Carlos')")