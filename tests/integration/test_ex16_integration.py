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

def test_ex16_convert_parquet_to_iceberg():
    m = importlib.import_module("exercicios.ex16")
    s = spark()
    m.ex16_convert_parquet_to_iceberg(s, "lab.db.pq_table", "/tmp/pq")
    assert True