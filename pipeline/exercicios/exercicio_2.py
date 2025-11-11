# Exercício 2: Salvar DataFrame no HDFS como CSV
# Crie um DataFrame e salve em hdfs://namenode:9000/data/ex1.csv no formato CSV.

from pyspark.sql import SparkSession

# Criar sessão Spark
spark = SparkSession.builder.appName("Exercicio2").getOrCreate()

# Criar DataFrame
data = [(1, "João"), (2, "Maria"), (3, "Pedro")]
columns = ["id", "nome"]
df = spark.createDataFrame(data, columns)

# Salvar no HDFS como CSV
df.write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/data/ex1.csv")

print("DataFrame salvo no HDFS como CSV")

# Parar sessão
spark.stop()