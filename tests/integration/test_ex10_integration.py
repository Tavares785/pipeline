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

def test_ex10_select_ano():
    m = importlib.import_module("exercicios.ex10")
    s = spark()
    df = m.ex10_select_ano(s, 2023)
    assert df.count() >= 0  # deve rodar sem erro
