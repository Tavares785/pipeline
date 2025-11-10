from pyspark.sql import SparkSession

def ex12_create_df_table(spark: SparkSession) -> None:
    """
    Cria DataFrame e salva como lab.db.tabela_df via writeTo.
    """
    dados = [(1, "Narumi"), (2, "Delta"), (3, "Vini")]
    df = spark.createDataFrame(dados, ["id", "nome"])
    

    df.writeTo("lab.db.tabela_df").using("iceberg").createOrReplace()
