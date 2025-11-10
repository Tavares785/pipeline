#!/usr/bin/env python3
"""
Exercício 2: Salvar DataFrame no HDFS como CSV
Objetivo: Criar um DataFrame e salvar em hdfs://namenode:9000/data/ex1.csv no formato CSV.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # Criar sessão Spark
    spark = SparkSession.builder \
        .appName("Exercicio02_Salvar_CSV_HDFS") \
        .getOrCreate()
    
    try:
        print("💾 Exercício 2: Salvando DataFrame no HDFS como CSV...")
        
        # Definir dados e schema
        data_ex2 = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Diana")]
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("nome", StringType(), True)
        ])
        
        # Criar DataFrame
        df_ex2 = spark.createDataFrame(data_ex2, schema)
        
        print("📊 DataFrame a ser salvo:")
        df_ex2.show()
        
        # Caminho HDFS
        hdfs_path = "hdfs://namenode:9000/data/ex1.csv"
        
        # Salvar como CSV no HDFS
        df_ex2.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(hdfs_path)
        
        print(f"✅ DataFrame salvo com sucesso em: {hdfs_path}")
        
        # Verificar se o arquivo foi criado
        try:
            import subprocess
            result = subprocess.run(['hdfs', 'dfs', '-ls', '/data/'], 
                                  capture_output=True, text=True)
            print("\n📁 Arquivos no diretório /data/:")
            print(result.stdout)
        except Exception as e:
            print(f"⚠️ Não foi possível listar arquivos: {e}")
        
        return hdfs_path
        
    except Exception as e:
        print(f"❌ Erro no exercício 2: {e}")
        return None
    finally:
        spark.stop()

if __name__ == "__main__":
    main()