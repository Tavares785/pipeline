from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BigDataLab") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
    .getOrCreate()

data = [(1, "Papai123"), (2, "EraUmaVez"), (3, "Kazuma")]
df = spark.createDataFrame(data, ["id", "nome"])
df.show()
