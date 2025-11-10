df_csv = spark.read.csv("hdfs://namenode:9000/data/ex1.csv", header=True, inferSchema=True)
df_csv.show()
