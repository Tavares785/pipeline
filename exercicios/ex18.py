df_vendas = spark.sql("SELECT * FROM lab.db.vendas")
df_vendas.write.mode("overwrite").csv("hdfs://namenode:9000/export/vendas.csv", header=True)