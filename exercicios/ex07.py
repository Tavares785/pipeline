from pyspark.sql import SparkSession

TABLE = "lab.db.pessoas"

def ex07_delete_bob(spark: SparkSession) -> None:
    """
    Remove linhas onde nome = 'Vini'.
    """
    spark.sql(f"""
        DELETE FROM {TABLE}
        WHERE nome = 'Vini'
    """)
