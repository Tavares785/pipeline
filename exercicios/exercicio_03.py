#!/usr/bin/env python3
"""
Exercício 3: Ler CSV do HDFS
Objetivo: Ler o arquivo salvo no exercício anterior usando spark.read.csv() e exibir o DataFrame.
"""

from pyspark.sql import SparkSession

def main():
    # Criar sessão Spark
    spark = SparkSession.builder \
        .appName("Exercicio03_Ler_CSV_HDFS") \
        .getOrCreate()
    
    try:
        print("📖 Exercício 3: Lendo CSV do HDFS...")
        
        # Caminho do arquivo CSV no HDFS
        hdfs_path = "hdfs://namenode:9000/data/ex1.csv"
        
        # Ler CSV do HDFS
        df_lido = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(hdfs_path)
        
        print("✅ DataFrame lido com sucesso!")
        print("\n📊 Dados lidos do HDFS:")
        df_lido.show()
        
        print("\n📋 Schema inferido:")
        df_lido.printSchema()
        
        print(f"\n📈 Número de registros lidos: {df_lido.count()}")
        
        # Mostrar algumas estatísticas
        print("\n📊 Estatísticas básicas:")
        df_lido.describe().show()
        
        return df_lido
        
    except Exception as e:
        print(f"❌ Erro no exercício 3: {e}")
        return None
    finally:
        spark.stop()

if __name__ == "__main__":
    main()