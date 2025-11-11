from pyspark.sql import SparkSession

def ex11_history_and_detail(spark: SparkSession) -> dict:
    """
    Retorna dicionário: {"history": df_history.count(), "detail": df_detail.collect()}
    """
    # TODO
    raise NotImplementedError


CREATE TABLE lab.db.vendas (
  id INT,
  valor DOUBLE,
  ano INT
)
USING ICEBERG
PARTITIONED BY (ano);
