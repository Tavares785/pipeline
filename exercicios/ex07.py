from pyspark.sql import SparkSession

TABLE = "lab.db.pessoas"

def ex07_delete_bob(spark: SparkSession) -> None:
    """
    Remove linhas onde nome = 'Bob'.
    """
    spark.sql(f"""
        DELETE FROM {TABLE}
        WHERE nome = 'Bob'
    """)
