from pyspark.sql import SparkSession

def ex11_history_and_detail(spark: SparkSession) -> dict:
    """
    Retorna dicionário: {"history": df_history.count(), "detail": df_detail.collect()}
    """
    df_history = spark.sql("SELECT * FROM lab.db.vendas")
    
    df_detail = spark.sql("SELECT * FROM lab.db.vendas WHERE quantidade > 10")
    
    return {
        "history": df_history.count(),
        "detail": df_detail.collect()
    }
