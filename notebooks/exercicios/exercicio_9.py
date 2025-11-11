# Exercício 9: Atualizar um registro
# Atualize um nome usando Iceberg SQL.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio9") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Atualizar registro
spark.sql("UPDATE lab.db.pessoas SET nome = 'Alice Silva' WHERE nome = 'Alice'")

print("Registro atualizado com sucesso")

# Verificar resultado
result = spark.sql("SELECT * FROM lab.db.pessoas")
result.show()

# Parar sessão
spark.stop()