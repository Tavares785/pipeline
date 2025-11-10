from pyspark.sql import SparkSession

def ex09_insert_vendas(spark: SparkSession) -> None:
    """
    Insere registros na lab.db.vendas variando o ano.
    """
    spark.sql("""
        INSERT INTO lab.db.vendas VALUES
        (1, 'Produto A', 10, 2023),
        (2, 'Produto B', 5, 2023),
        (3, 'Produto C', 8, 2024),
        (4, 'Produto D', 12, 2024),
        (5, 'Produto E', 7, 2025)
    """)
