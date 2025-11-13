from pyspark.sql import SparkSession

def ex14_export_vendas_csv(spark: SparkSession, path: str) -> None:
    """
    Salva lab.db.vendas como CSV no path.
    """
    # Lê a tabela lab.db.vendas
    df = spark.table("lab.db.vendas")
    
    # Salva em formato CSV, com cabeçalho
    df.write.mode("overwrite").option("header", True).csv(path)
