from pyspark.sql import SparkSession

TABLE = "lab.db.pessoas"

def ex06_update_name(spark: SparkSession) -> None:
    """
    Atualiza 'Alice' para 'Alice Silva' na tabela lab.db.pessoas.
    """
    spark.sql(f"""
        UPDATE {TABLE}
        SET nome = 'Alice Silva'
        WHERE nome = 'Alice'
    """)
