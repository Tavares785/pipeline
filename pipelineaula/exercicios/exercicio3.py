# Exercício 3: Ler CSV do HDFS
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercicio3").getOrCreate()

df_lido = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://namenode:9000/data/ex1.csv")
df_lido.show()
df_lido.printSchema()