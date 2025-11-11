from pyspark.sql import SparkSession, DataFrame

def ex03_read_csv(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Lê CSV gerado no ex02 e retorna um DataFrame com as mesmas colunas.
    """
    # TODO: implementar
    raise NotImplementedError


spark = SparkSession.builder \
    .appName("Exercicio2") \
    .getOrCreate()

path = "hdfs://namenode:9000/data/ex1.csv"

df_spark = spark.read.csv(path, header=True, inferSchema=True)

df_spark.show()