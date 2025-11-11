from pyspark.sql import SparkSession

def ex18_merge_into(spark: SparkSession) -> None:
    """
    Executa MERGE INTO em lab.db.vendas atualizando valores.
    """
    # TODO
    raise NotImplementedError

df = spark.read.format("iceberg").load("lab.db.vendas")
df.write.csv("hdfs://namenode:9000/export/vendas.csv", header=True)
