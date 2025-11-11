from pyspark.sql import SparkSession

def ex02_save_csv(spark: SparkSession, output_path: str) -> None:
    """
    Cria um DataFrame e salva em CSV em output_path.
    Deve gerar header.
    """
    # TODO: implementar
    raise NotImplementedError

df = pd.DataFrame({
    'id': [1, 2, 3],
    'nome': ['Ana', 'Bruno', 'Carlos']
})

path = "hdfs://namenode:9000/data/ex1.csv"

df.to_csv(path, index=False)