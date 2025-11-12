from pyspark.sql import SparkSession

def ex19_rewrite_data_files(spark: SparkSession) -> None:
    """
    Executa otimização (compaction) na tabela lab.db.vendas:
    CALL lab.system.rewrite_data_files(table => 'lab.db.vendas');
    """
    spark.sql("""
        CALL lab.system.rewrite_data_files(
            table => 'lab.db.vendas'
        )
    """)
