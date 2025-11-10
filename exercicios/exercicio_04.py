#!/usr/bin/env python3
"""
Exercício 4: Criar namespace Iceberg
Objetivo: Criar um namespace chamado lab.db no catálogo Iceberg.
"""

from pyspark.sql import SparkSession

def main():
    # Criar sessão Spark com configuração Iceberg
    spark = SparkSession.builder \
        .appName("Exercicio04_Namespace_Iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
        .getOrCreate()
    
    try:
        print("🗂️ Exercício 4: Criando namespace Iceberg...")
        
        # Verificar catálogos disponíveis
        print("\n📋 Catálogos disponíveis:")
        spark.sql("SHOW CATALOGS").show()
        
        # Criar namespace lab
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.lab")
        print("✅ Namespace 'lab' criado!")
        
        # Criar namespace lab.db
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.lab.db")
        print("✅ Namespace 'lab.db' criado!")
        
        # Verificar namespaces criados
        print("\n📋 Namespaces disponíveis no catálogo iceberg:")
        try:
            spark.sql("SHOW NAMESPACES IN iceberg").show()
        except Exception as e:
            print(f"⚠️ Erro ao mostrar namespaces: {e}")
            # Alternativa
            spark.sql("SHOW DATABASES").show()
        
        print("🎉 Exercício 4 concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro no exercício 4: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()