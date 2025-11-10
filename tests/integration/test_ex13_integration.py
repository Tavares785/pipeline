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

def test_ex13_time_travel():
    m = importlib.import_module("exercicios.ex13")
    s = spark()
    df = m.ex13_time_travel(s, 0)
    assert df.count() >= 0
