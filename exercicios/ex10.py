from pyspark.sql import SparkSession, DataFrame

def ex10_select_ano(spark: SparkSession, ano: int) -> DataFrame:
    """
    Retorna apenas vendas do ano informado.
    """
    df = spark.sql(f"SELECT * FROM lab.db.vendas WHERE ano = {ano}")
    return df
