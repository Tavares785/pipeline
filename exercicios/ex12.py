from pyspark.sql import SparkSession

def ex12_create_df_table(spark: SparkSession) -> None:
    """
    Cria DataFrame e salva como lab.db.tabela_df via writeTo.
    """
    # Cria um DataFrame de exemplo
    data = [
        (10, 500.0, 2025),
        (11, 750.0, 2025)
    ]
    columns = ["id", "valor", "ano"]
    df = spark.createDataFrame(data, columns)

    # Grava como tabela Iceberg usando writeTo()
    df.writeTo("lab.db.tabela_df").createOrReplace()
