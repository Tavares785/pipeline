spark.sql("""
CREATE TABLE IF NOT EXISTS lab.db.pessoas (
    id INT,
    nome STRING
) USING ICEBERG
""")
