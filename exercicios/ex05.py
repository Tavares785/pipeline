from pyspark.sql import SparkSession

TABLE = "lab.db.pessoas"

def ex05_insert_and_count(spark: SparkSession) -> int:
    """
    Faz INSERT de 3 linhas na tabela lab.db.pessoas e retorna a contagem (int).
    """
    # Inserir 3 registros na tabela Iceberg
    spark.sql(f"""
        INSERT INTO {TABLE} VALUES
            (1, 'Alice'),
            (2, 'Bruno'),
            (3, 'Carla')
    """)

    # Contar quantos registros existem na tabela
    result = spark.sql(f"SELECT COUNT(*) AS total FROM {TABLE}")
    count = result.collect()[0]["total"]

    return count

