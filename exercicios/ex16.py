from pyspark.sql import SparkSession

def ex16_convert_parquet_to_iceberg(spark: SparkSession, table: str, path: str) -> None:
    """
    Converte tabela Parquet em Iceberg SET TBLPROPERTIES('format-version'='2').
    """
    # TODO
    raise NotImplementedError

CREATE TABLE lab.db.parquet_vendas (
    id INT,
    valor DOUBLE,
    ano INT
)
USING PARQUET;

ALTER TABLE lab.db.parquet_vendas
SET TBLPROPERTIES ('format-version'='2');
