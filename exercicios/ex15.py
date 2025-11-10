df2 = spark.createDataFrame([(10, "Daniela"), (11, "Eduardo")], ["id", "nome"])
df2.writeTo("lab.db.tabela_df").createOrReplace()
spark.sql("SELECT * FROM lab.db.tabela_df").show()
