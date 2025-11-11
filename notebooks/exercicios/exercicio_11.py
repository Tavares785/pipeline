# Exercício 11: Criar tabela particionada
# Crie uma tabela Iceberg com partição por ano.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio11") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Criar tabela particionada
spark.sql("""
CREATE TABLE IF NOT EXISTS lab.db.vendas (
    id INT,
    valor DOUBLE,
    ano INT
) USING ICEBERG
PARTITIONED BY (ano)
""")

print("Tabela particionada lab.db.vendas criada com sucesso")

# Parar sessão
spark.stop()