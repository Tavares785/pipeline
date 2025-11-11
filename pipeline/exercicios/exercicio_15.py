# Exercício 15: Criar tabela Iceberg a partir de DataFrame
# Crie um DataFrame artificial e grave diretamente.

from pyspark.sql import SparkSession

# Criar sessão Spark com configuração Iceberg
spark = SparkSession.builder \
    .appName("Exercicio15") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Criar DataFrame artificial
data = [(1, "Produto A", 25.50), (2, "Produto B", 45.75), (3, "Produto C", 12.30)]
columns = ["id", "produto", "preco"]
df = spark.createDataFrame(data, columns)

# Gravar como tabela Iceberg
df.writeTo("lab.db.tabela_df").createOrReplace()

print("Tabela Iceberg criada a partir de DataFrame")

# Verificar resultado
result = spark.sql("SELECT * FROM lab.db.tabela_df")
result.show()

# Parar sessão
spark.stop()