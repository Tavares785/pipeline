# Exercício 16: Converter tabela para Iceberg
# Crie uma tabela Parquet simples e converta para Iceberg.

from pyspark.sql import SparkSession

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("Exercicio16") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Criar DataFrame e salvar como Parquet
data = [(1, "Item A"), (2, "Item B"), (3, "Item C")]
columns = ["id", "item"]
df = spark.createDataFrame(data, columns)

# Salvar como tabela Parquet
df.write.mode("overwrite").saveAsTable("lab.db.tabela_parquet")

# Converter para Iceberg (formato v2)
spark.sql("ALTER TABLE lab.db.tabela_parquet SET TBLPROPERTIES ('format-version'='2')")

print("Tabela convertida para Iceberg formato v2")

# Verificar resultado
result = spark.sql("SELECT * FROM lab.db.tabela_parquet")
result.show()

# Parar sessão
spark.stop()