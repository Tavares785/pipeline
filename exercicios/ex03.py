from pyspark.sql import SparkSession, DataFrame

def ex03_read_csv(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Lê CSV gerado no ex02 e retorna um DataFrame com as mesmas colunas.
    """
    # Lê o CSV com cabeçalho
    df = spark.read.option("header", "true").csv(input_path)
    return df
