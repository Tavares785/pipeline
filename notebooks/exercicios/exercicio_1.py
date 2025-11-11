# Exercício 1: Criar um DataFrame simples
# Crie um DataFrame com três linhas e duas colunas (id, nome) e mostre seu conteúdo.

from pyspark.sql import SparkSession

# Criar sessão Spark
spark = SparkSession.builder.appName("Exercicio1").getOrCreate()

# Criar DataFrame com dados simples
data = [(1, "João"), (2, "Maria"), (3, "Pedro")]
columns = ["id", "nome"]

df = spark.createDataFrame(data, columns)

# Mostrar conteúdo
df.show()

# Parar sessão
spark.stop()