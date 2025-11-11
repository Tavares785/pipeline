from pyspark.sql import SparkSession

def ex15_create_parquet_table(spark: SparkSession, path: str) -> None:
    """
    Cria um DataFrame simples e salva como Parquet.
    """
    # TODO
    raise NotImplementedError

from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("WriteDataFrameToIceberg").getOrCreate()

data = [(1, "Produto A", 10.5), (2, "Produto B", 15.8)]
columns = ["id", "produto", "valor"]
df = spark.createDataFrame(data, columns)

df.writeTo("lab.db.tabela_df").createOrReplace()
