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

def test_ex15_create_parquet_table():
    m = importlib.import_module("exercicios.ex15")
    s = spark()
    m.ex15_create_parquet_table(s, "/tmp/pq")
    assert True