# Exercício 15: Criar tabela Iceberg a partir de DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Exercicio15") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lab.type", "hive") \
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083") \
    .getOrCreate()

data_df = [(1, "Produto A", 100.0), (2, "Produto B", 200.0), (3, "Produto C", 150.0)]
schema_produtos = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("preco", DoubleType(), True)
])

df_produtos = spark.createDataFrame(data_df, schema_produtos)
df_produtos.writeTo("lab.db.tabela_df").createOrReplace()
spark.sql("SELECT * FROM lab.db.tabela_df").show()