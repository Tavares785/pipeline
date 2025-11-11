# Exercício 5: Criar tabela Iceberg
# Crie uma tabela Iceberg lab.db.pessoas (id INT, nome STRING) usando SQL.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio5") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Criar tabela Iceberg
spark.sql("""
CREATE TABLE IF NOT EXISTS lab.db.pessoas (
    id INT,
    nome STRING
) USING ICEBERG
""")

print("Tabela lab.db.pessoas criada com sucesso")

# Parar sessão
spark.stop()