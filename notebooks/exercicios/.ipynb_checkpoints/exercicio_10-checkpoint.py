# Exercício 10: Deletar um registro
# Remova uma linha da tabela usando DELETE FROM.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio10") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Deletar registro
spark.sql("DELETE FROM lab.db.pessoas WHERE nome = 'Bob'")

print("Registro deletado com sucesso")

# Verificar resultado
result = spark.sql("SELECT * FROM lab.db.pessoas")
result.show()

# Parar sessão
spark.stop()