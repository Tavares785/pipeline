from pyspark.sql import SparkSession

TABLE = "lab.db.pessoas"

def ex06_update_name(spark: SparkSession) -> None:
    """
    Atualiza 'Alice' para 'Alice Silva' na tabela lab.db.pessoas.
    """
    # TODO:
    # spark.sql("UPDATE lab.db.pessoas SET nome='Alice Silva' WHERE nome='Alice'")
    raise NotImplementedError


INSERT INTO lab.db.pessoas VALUES
    (1, 'Ana'),
    (2, 'Bruno'),
    (3, 'Carlos');
