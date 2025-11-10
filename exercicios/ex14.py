spark.sql("DESCRIBE HISTORY lab.db.vendas").show(truncate=False)
spark.sql("DESCRIBE DETAIL lab.db.vendas").show(truncate=False)