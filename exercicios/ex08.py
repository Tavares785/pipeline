from pyspark.sql import SparkSession

def ex08_create_partitioned_table(spark: SparkSession) -> None:
    """
    Cria tabela Iceberg lab.db.vendas particionada por ano.
    """
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lab.db.vendas (
            id INT,
            valor DOUBLE,
            ano INT
        )
        USING ICEBERG
        PARTITIONED BY (ano)
    """)
