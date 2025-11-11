from pyspark.sql import SparkSession

TABLE = "lab.db.pessoas"

def ex05_insert_and_count(spark: SparkSession) -> int:
    """
    Faz INSERT de 3 linhas na tabela lab.db.pessoas e retorna a contagem (int).
    """
    # TODO: implementar
    raise NotImplementedError


CREATE TABLE lab.db.pessoas (
    id INT,
    nome STRING
)
USING ICEBERG;