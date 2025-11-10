from pyspark.sql import SparkSession

def ex17_delete_and_vacuum(spark: SparkSession) -> None:
    """
    Deleta linhas por condição e roda:
    CALL lab.system.expire_snapshots(...)
    """
    spark.sql("""
        DELETE FROM lab.db.vendas_iceberg
        WHERE ano < 2023
    """)
    
    spark.sql("""
        CALL lab.system.expire_snapshots(
            table => 'lab.db.vendas_iceberg',
            older_than => TIMESTAMPADD(DAY, -1, CURRENT_TIMESTAMP())
        )
    """)
