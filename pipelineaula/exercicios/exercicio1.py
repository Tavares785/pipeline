# Exercício 1: Criar um DataFrame simples
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Exercicio1").getOrCreate()

data = [(1, "João"), (2, "Maria"), (3, "Pedro")]
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()