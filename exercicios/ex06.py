from pyspark.sql import SparkSession

TABLE = "lab.db.pessoas"

def ex06_update_name(spark: SparkSession) -> None:
    """
    Atualiza 'Narumi' para 'Narumi Giovana' na tabela lab.db.pessoas.
    """
    spark.sql(f"""
        UPDATE {TABLE}
        SET nome = 'Narumi Giovana'
        WHERE nome = 'Narumi'
    """)
