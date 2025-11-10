from pyspark.sql import SparkSession

def ex02_save_csv(spark: SparkSession, output_path: str) -> None:
    """
    Cria um DataFrame e salva em CSV em output_path.
    Deve gerar header.
    """
    df = spark.createDataFrame(
        [(1, "Narumi"), (2, "Delta"), (3, "Vini")],
        ["id", "nome"]
    )
    df.write.mode("overwrite").option("header", True).csv(output_path)
