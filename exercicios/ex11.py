from pyspark.sql import SparkSession

def ex11_history_and_detail(spark: SparkSession) -> dict:
    """
    Retorna dicionário: {"history": df_history.count(), "detail": df_detail.collect()}
    """
    # Consulta o histórico de versões da tabela Iceberg
    df_history = spark.sql("DESCRIBE HISTORY lab.db.vendas")

    # Consulta detalhes técnicos da tabela (metadados)
    df_detail = spark.sql("DESCRIBE DETAIL lab.db.vendas")

    # Monta o dicionário com as informações solicitadas
    return {
        "history": df_history.count(),
        "detail": df_detail.collect()
    }
