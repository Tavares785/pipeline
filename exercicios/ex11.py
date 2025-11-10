spark.sql("""
CREATE TABLE IF NOT EXISTS lab.db.vendas (
  id INT,
  valor DOUBLE,
  ano INT
) USING ICEBERG
PARTITIONED BY (ano)
""")
