# Exercício 18: Exportar tabela Iceberg para CSV
# Leia a tabela Iceberg e salve para HDFS.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio18") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Ler tabela Iceberg
df = spark.sql("SELECT * FROM lab.db.vendas")

# Exportar para CSV no HDFS
df.write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/export/vendas.csv")

print("Tabela Iceberg exportada para CSV no HDFS")

# Parar sessão
spark.stop()