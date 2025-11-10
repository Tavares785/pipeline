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

def test_ex06_update_name():
    m = importlib.import_module("exercicios.ex06")
    s = spark()
    m.ex06_update_name(s)
    r = s.sql("SELECT nome FROM lab.db.pessoas WHERE nome='Alice Silva'").count()
    assert r >= 1
