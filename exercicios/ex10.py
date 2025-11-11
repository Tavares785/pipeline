from pyspark.sql import SparkSession, DataFrame

def ex10_select_ano(spark: SparkSession, ano: int) -> DataFrame:
    """
    Retorna apenas vendas do ano informado.
    """
    # TODO
    raise NotImplementedError


DELETE FROM lab.db.pessoas
WHERE nome = 'Carlos';
