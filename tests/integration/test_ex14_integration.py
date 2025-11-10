import importlib, os
from pyspark.sql import SparkSession

def spark():
    return (
        SparkSession.builder
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hive")
        .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

def test_ex14_export_vendas_csv(tmp_path):
    m = importlib.import_module("exercicios.ex14")
    s = spark()
    path = f"hdfs://namenode:9000/tmp/vendas_export.csv"
    m.ex14_export_vendas_csv(s, path)
    # Se não falhou, está ok
    assert True
