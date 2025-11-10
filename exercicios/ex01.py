from pyspark.sql import SparkSession, DataFrame

def ex01_create_df(spark: SparkSession) -> DataFrame:
    """
    Deve criar um DataFrame com colunas (id, nome) e 3 linhas.
    """
    return spark.createDataFrame(
        [(1, "Narumi"), (2, "Delta"), (3, "Vini")],
        ["id", "nome"]
    )
