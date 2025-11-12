from pyspark.sql import SparkSession

def ex16_convert_parquet_to_iceberg(spark: SparkSession, table: str, path: str) -> None:
    """
    Converte tabela Parquet em Iceberg e define 'format-version' = '2'.
    """
    # Cria a tabela temporária baseada nos arquivos Parquet
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING PARQUET
        LOCATION '{path}'
    """)

    # Converte a tabela Parquet para Iceberg (versão 2)
    spark.sql(f"""
        ALTER TABLE {table}
        SET TBLPROPERTIES ('format-version'='2')
    """)
