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

def test_ex09_insert_vendas():
    m = importlib.import_module("exercicios.ex09")
    s = spark()
    m.ex09_insert_vendas(s)
    assert s.sql("SELECT COUNT(*) FROM lab.db.vendas").collect()[0][0] >= 1
