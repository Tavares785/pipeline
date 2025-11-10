spark.sql("CREATE TABLE IF NOT EXISTS lab.db.parquet_table (id INT, nome STRING) USING PARQUET")

spark.sql("ALTER TABLE lab.db.parquet_table SET TBLPROPERTIES ('format-version'='2')")
