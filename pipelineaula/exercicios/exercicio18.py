# Exercício 18: Exportar tabela Iceberg para CSV
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio18") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

vendas_df = spark.sql("SELECT * FROM lab.db.vendas")
vendas_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/export/vendas.csv")