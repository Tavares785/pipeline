from pyspark.sql import SparkSession

TABLE = "lab.db.pessoas"

def ex05_insert_and_count(spark: SparkSession) -> int:
    """
    Faz INSERT de 3 linhas na tabela lab.db.pessoas e retorna a contagem (int).
    """
    spark.sql(f"""
        INSERT INTO {TABLE} VALUES
        (1, "Narumi"), (2, "Delta"), (3, "Vini")
    """)

    result = spark.sql(f"SELECT COUNT(*) AS total FROM {TABLE}").collect()[0]["total"]
    return result
