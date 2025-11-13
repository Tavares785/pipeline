#!/usr/bin/env python3
"""
Exercício 1: Criar um DataFrame simples
Objetivo: Criar um DataFrame com três linhas e duas colunas (id, nome) e mostrar seu conteúdo.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # Criar sessão Spark
    spark = SparkSession.builder \
        .appName("Exercicio01_DataFrame_Simples") \
        .getOrCreate()
    
    try:
        print("📊 Exercício 1: Criando DataFrame simples...")
        
        # Definir dados e schema
        data = [(1, "João"), (2, "Maria"), (3, "Pedro")]
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("nome", StringType(), True)
        ])
        
        # Criar DataFrame
        df_simples = spark.createDataFrame(data, schema)
        
        print("✅ DataFrame criado com sucesso!")
        print("\n📋 Conteúdo do DataFrame:")
        df_simples.show()
        
        print("\n📊 Schema do DataFrame:")
        df_simples.printSchema()
        
        print(f"\n📈 Número de registros: {df_simples.count()}")
        
        return df_simples
        
    except Exception as e:
        print(f"❌ Erro no exercício 1: {e}")
        return None
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
