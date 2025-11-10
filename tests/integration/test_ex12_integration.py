import importlib
from pyspark.sql import SparkSession

def spark():
    return (
        SparkSession.builder
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hive")
        .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

def test_ex12_create_df_table():
    m = importlib.import_module("exercicios.ex12")
    s = spark()
    m.ex12_create_df_table(s)
    tables = s.sql("SHOW TABLES IN lab.db").collect()
    assert any(r.tableName == "tabela_df" for r in tables)
