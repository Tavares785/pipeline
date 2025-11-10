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

def test_ex07_delete_bob():
    m = importlib.import_module("exercicios.ex07")
    s = spark()
    m.ex07_delete_bob(s)
    r = s.sql("SELECT nome FROM lab.db.pessoas WHERE nome='Bob'").count()
    assert r == 0
