# Exercício 2: Salvar DataFrame no HDFS como CSV
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Exercicio2").getOrCreate()

data = [(1, "João"), (2, "Maria"), (3, "Pedro")]
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True)
])
df = spark.createDataFrame(data, schema)

df.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/data/ex1.csv")