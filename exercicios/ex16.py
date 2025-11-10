from pyspark.sql import SparkSession

def ex16_convert_parquet_to_iceberg(spark: SparkSession, table: str, path: str) -> None:
    """
    Converte tabela Parquet em Iceberg SET TBLPROPERTIES('format-version'='2').
    """
    df = spark.read.parquet(path)
    
    (
        df.writeTo(table)
          .using("iceberg")
          .tableProperty("format-version", "2")
          .createOrReplace()
    )
