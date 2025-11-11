# Exercício 11: Criar tabela particionada
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio11") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

spark.sql("""
CREATE TABLE IF NOT EXISTS lab.db.vendas (
    id INT,
    valor DOUBLE,
    ano INT
) USING ICEBERG
PARTITIONED BY (ano)
""")