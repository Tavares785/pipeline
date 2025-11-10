from pyspark.sql import SparkSession, DataFrame

def ex01_create_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([(1,"Alice"), (2,"Bob"), (3,"Carol")], ["id", "nome"])
    raise NotImplementedError
