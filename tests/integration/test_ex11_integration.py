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

def test_ex11_history_and_detail():
    m = importlib.import_module("exercicios.ex11")
    s = spark()
    r = m.ex11_history_and_detail(s)
    assert isinstance(r, dict)
