#!/usr/bin/env python3
"""
Exercício 6: Inserir dados na tabela Iceberg
Objetivo: Inserir 3 valores manualmente usando SQL INSERT INTO.
"""

from pyspark.sql import SparkSession

def main():
    # Criar sessão Spark com configuração Iceberg
    spark = SparkSession.builder \
        .appName("Exercicio06_Inserir_Dados_Iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
        .getOrCreate()
    
    try:
        print("📝 Exercício 6: Inserindo dados na tabela Iceberg...")
        
        # SQL para inserir dados
        insert_sql = """
        INSERT INTO iceberg.lab.db.pessoas VALUES 
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie')
        """
        
        # Executar inserção
        spark.sql(insert_sql)
        print("✅ Dados inseridos com sucesso!")
        
        # Verificar dados inseridos
        print("\n📊 Dados na tabela pessoas:")
        spark.sql("SELECT * FROM iceberg.lab.db.pessoas").show()
        
        # Contar registros
        count = spark.sql("SELECT COUNT(*) as total FROM iceberg.lab.db.pessoas").collect()[0]['total']
        print(f"\n📈 Total de registros inseridos: {count}")
        
        print("🎉 Exercício 6 concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro no exercício 6: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()