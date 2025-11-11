from pyspark.sql import SparkSession

def ex08_create_partitioned_table(spark: SparkSession) -> None:
    """
    Cria tabela Iceberg lab.db.vendas particionada por ano.
    """
    # TODO
    raise NotImplementedError


SELECT COUNT(*) AS total_registros
FROM lab.db.pessoas;
