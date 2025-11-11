# Exercício 12: Inserir dados particionados
# Insira dados variando o valor de ano.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio12") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Inserir dados com diferentes anos
spark.sql("INSERT INTO lab.db.vendas VALUES (1, 1500.50, 2022)")
spark.sql("INSERT INTO lab.db.vendas VALUES (2, 2300.75, 2023)")
spark.sql("INSERT INTO lab.db.vendas VALUES (3, 1800.25, 2023)")
spark.sql("INSERT INTO lab.db.vendas VALUES (4, 2100.00, 2024)")

print("Dados particionados inseridos na tabela lab.db.vendas")

# Verificar dados
result = spark.sql("SELECT * FROM lab.db.vendas")
result.show()

# Parar sessão
spark.stop()