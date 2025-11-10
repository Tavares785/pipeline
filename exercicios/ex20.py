from pyspark.sql import SparkSession, DataFrame

def ex20_sql_select(spark: SparkSession) -> DataFrame:
    """
    SELECT * FROM lab.db.pessoas
    """
    df = spark.sql("SELECT * FROM lab.db.pessoas")
    return df
