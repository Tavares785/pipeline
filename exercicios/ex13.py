from pyspark.sql import SparkSession, DataFrame

def ex13_time_travel(spark: SparkSession, version: int) -> DataFrame:
    """
    Retorna SELECT * FROM lab.db.vendas VERSION AS OF {version}
    """
    # TODO
    raise NotImplementedError
