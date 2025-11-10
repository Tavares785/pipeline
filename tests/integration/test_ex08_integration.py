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

def test_ex08_create_partitioned_table():
    m = importlib.import_module("exercicios.ex08")
    s = spark()
    m.ex08_create_partitioned_table(s)
    tables = s.sql("SHOW TABLES IN lab.db").collect()
    assert any(r.tableName == "vendas" for r in tables)
