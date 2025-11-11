from pyspark.sql import SparkSession

NAMESPACE = "lab.db"
TABLE = "lab.db.pessoas"

def ex04_create_namespace_and_table(spark: SparkSession) -> None:
    """
    Cria o namespace lab.db e a tabela lab.db.pessoas(id INT, nome STRING) no Iceberg.
    Usa spark.sql(...).
    """
    # TODO: implementar
    raise NotImplementedError


spark = SparkSession.builder \
    .appName("CriarNamespaceIceberg") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.lab.db")