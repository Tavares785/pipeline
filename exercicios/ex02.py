from pyspark.sql import SparkSession

def ex02_save_csv(spark: SparkSession, output_path: str) -> None:
    """
    Cria um DataFrame e salva em CSV em output_path.
    Deve gerar header.
    """
    # Cria o DataFrame de exemplo
    data = [
        (1, "Alice"),
        (2, "Bruno"),
        (3, "Carla")
    ]
    columns = ["id", "nome"]
    df = spark.createDataFrame(data, columns)
    
    # Salva no caminho informado (com cabeçalho)
    df.write.mode("overwrite").option("header", "true").csv(output_path)
