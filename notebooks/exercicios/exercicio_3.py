# Exercício 3: Ler CSV do HDFS
# Leia o arquivo salvo no exercício anterior usando spark.read.csv() e exiba o DataFrame.

from pyspark.sql import SparkSession

# Criar sessão Spark
spark = SparkSession.builder.appName("Exercicio3").getOrCreate()

# Ler CSV do HDFS
df = spark.read.option("header", "true").csv("hdfs://namenode:9000/data/ex1.csv")

# Exibir DataFrame
df.show()

# Parar sessão
spark.stop()