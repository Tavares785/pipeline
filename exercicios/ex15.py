from pyspark.sql import SparkSession
from pyspark.sql import Row

def ex15_create_parquet_table(spark: SparkSession, path: str) -> None:
    """
    Cria um DataFrame simples e salva como Parquet.
    """
    dados = [
        Row(id=1, nome="Narumi", idade=25),
        Row(id=2, nome="Delta", idade=30),
        Row(id=3, nome="Vini", idade=22)
    ]
    
    df = spark.createDataFrame(dados)
    
    df.write.mode("overwrite").parquet(path)
