from pyspark.sql import SparkSession, DataFrame

def ex03_read_csv(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Lê CSV gerado no ex02 e retorna um DataFrame com as mesmas colunas.
    """
    df = (
        spark.read
        .option("header", True) 
        .option("inferSchema", True) 
        .csv(input_path)
    )
    return df
