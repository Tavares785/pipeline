from pyspark.sql import SparkSession, DataFrame

def ex20_sql_select(spark: SparkSession) -> DataFrame:
    """
    SELECT * FROM lab.db.pessoas
    """
    # Executa a consulta SQL e retorna o DataFrame
    return spark.sql("SELECT * FROM lab.db.pessoas")
