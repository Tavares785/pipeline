from pyspark.sql import SparkSession

def ex15_create_parquet_table(spark: SparkSession, path: str) -> None:
    """
    Cria um DataFrame simples e salva como Parquet.
    """
    # Cria um DataFrame de exemplo
    data = [
        (1, "Produto A", 100.0),
        (2, "Produto B", 150.5),
        (3, "Produto C", 200.0)
    ]
    columns = ["id",]()
