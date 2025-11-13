from pyspark.sql import SparkSession

def ex09_insert_vendas(spark: SparkSession) -> None:
    """
    Insere registros na lab.db.vendas variando o ano.
    """
    spark.sql("""
        INSERT INTO lab.db.vendas VALUES
            (1, 150.0, 2022),
            (2, 200.0, 2023),
            (3, 175.5, 2023),
            (4, 300.0, 2024)
    """)
