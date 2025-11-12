from pyspark.sql import SparkSession

def ex17_delete_and_vacuum(spark: SparkSession) -> None:
    """
    Deleta linhas por condição e roda:
    CALL lab.system.expire_snapshots('lab.db.vendas', TIMESTAMP '2025-01-01 00:00:00')
    """
    # Exemplo: deletar registros onde ano < 2023
    spark.sql("""
        DELETE FROM lab.db.vendas WHERE ano < 2023
    """)

    # Expira snapshots antigos (ajuste a data conforme necessário)
    spark.sql("""
        CALL lab.system.expire_snapshots(
            table => 'lab.db.vendas',
            older_than => TIMESTAMP '2025-01-01 00:00:00'
        )
    """)
