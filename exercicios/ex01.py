from pyspark.sql import SparkSession, DataFrame

def ex01_create_df(spark: SparkSession) -> DataFrame:
    """
    Deve criar um DataFrame com colunas (id, nome) e 3 linhas.
    """
    # TODO: implementar
    # Ex: return spark.createDataFrame([(1,"Alice"), (2,"Bob"), (3,"Carol")], ["id", "nome"])
    raise NotImplementedError

df = pd.DataFrame({
    'id': [1, 2, 3],
    'nome': ['Ana', 'Bruno', 'Carlos']
})