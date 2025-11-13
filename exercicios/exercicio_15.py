#!/usr/bin/env python3
"""
Exercício 15: Criar tabela Iceberg a partir de DataFrame
Objetivo: Criar um DataFrame artificial e gravar diretamente com df.writeTo().
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    # Criar sessão Spark com configuração Iceberg
    spark = SparkSession.builder \
        .appName("Exercicio15_DataFrame_Para_Iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
        .getOrCreate()
    
    try:
        print("🔄 Exercício 15: Criando tabela Iceberg a partir de DataFrame...")
        
        # Criar DataFrame artificial com dados de produtos
        data_produtos = [
            (1, "Notebook", 2500.00, "Eletrônicos"),
            (2, "Mouse", 50.00, "Eletrônicos"),
            (3, "Teclado", 150.00, "Eletrônicos"),
            (4, "Monitor", 800.00, "Eletrônicos"),
            (5, "Cadeira", 400.00, "Móveis"),
            (6, "Mesa", 600.00, "Móveis")
        ]
        
        schema_produtos = StructType([
            StructField("id", IntegerType(), True),
            StructField("nome", StringType(), True),
            StructField("preco", DoubleType(), True),
            StructField("categoria", StringType(), True)
        ])
        
        df_produtos = spark.createDataFrame(data_produtos, schema_produtos)
        
        print("📊 DataFrame produtos criado:")
        df_produtos.show()
        
        # Criar tabela Iceberg usando writeTo()
        df_produtos.writeTo("iceberg.lab.db.produtos").createOrReplace()
        print("✅ Tabela 'produtos' criada com sucesso a partir do DataFrame!")
        
        # Verificar tabela criada
        print("\n📋 Dados na nova tabela:")
        spark.sql("SELECT * FROM iceberg.lab.db.produtos").show()
        
        print("\n📊 Estatísticas por categoria:")
        spark.sql("""
            SELECT categoria, 
                   COUNT(*) as qtd_produtos,
                   AVG(preco) as preco_medio,
                   MAX(preco) as preco_maximo
            FROM iceberg.lab.db.produtos 
            GROUP BY categoria
        """).show()
        
        print("🎉 Exercício 15 concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro no exercício 15: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()