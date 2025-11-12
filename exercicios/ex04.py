from pyspark.sql import SparkSession

NAMESPACE = "lab.db"
TABLE = "lab.db.pessoas"

def ex04_create_namespace_and_table(spark: SparkSession) -> None:
    """
    Cria o namespace lab.db e a tabela lab.db.pessoas(id INT, nome STRING) no Iceberg.
    Usa spark.sql(...).
    """
    # Cria o namespace (se não existir)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

    # Cria a tabela Iceberg (se não existir)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id INT,
            nome STRING
        )
        USING ICEBERG
    """)
