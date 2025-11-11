# Exercício 9: Atualizar um registro
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio9") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

spark.sql("UPDATE lab.db.pessoas SET nome = 'Alice Silva' WHERE nome = 'Alice'")
spark.sql("SELECT * FROM lab.db.pessoas").show()