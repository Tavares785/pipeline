from pyspark.sql import SparkSession

def ex14_export_vendas_csv(spark: SparkSession, path: str) -> None:
    """
    Salva lab.db.vendas como CSV no path.
    """
    df = spark.table("lab.db.vendas")
    
    df.write.mode("overwrite").option("header", True).csv(path)
