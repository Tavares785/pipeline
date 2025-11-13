from pyspark.sql import SparkSession, DataFrame

def ex01_create_df(spark: SparkSession) -> DataFrame:
    """
    Deve criar um DataFrame com colunas (id, nome) e 3 linhas.
    """
    data = [
        (1, "Alice"),
        (2, "Bruno"),
        (3, "Carla")
    ]
    columns = ["id", "nome"]
    return spark.createDataFrame(data, columns)
