# Exercício 16: Converter tabela para Iceberg
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exercicio16") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

spark.sql("""
CREATE TABLE IF NOT EXISTS lab.db.temp_parquet (
    id INT,
    descricao STRING
) USING PARQUET
""")

spark.sql("INSERT INTO lab.db.temp_parquet VALUES (1, 'Teste')")

spark.sql("""
ALTER TABLE lab.db.temp_parquet 
SET TBLPROPERTIES ('format-version'='2')
""")