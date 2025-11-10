#!/usr/bin/env python3
"""
Exercício 18: Exportar tabela Iceberg para CSV
Objetivo: Ler a tabela Iceberg e salvar para hdfs://namenode:9000/export/vendas.csv.
"""

from pyspark.sql import SparkSession

def main():
    # Criar sessão Spark com configuração Iceberg
    spark = SparkSession.builder \
        .appName("Exercicio18_Exportar_Iceberg_CSV") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
        .getOrCreate()
    
    try:
        print("📤 Exercício 18: Exportando tabela Iceberg para CSV...")
        
        # Ler dados da tabela Iceberg
        df_vendas_export = spark.sql("SELECT * FROM iceberg.lab.db.vendas")
        
        print("📊 Dados a serem exportados:")
        df_vendas_export.show()
        
        # Caminho de destino no HDFS
        export_path = "hdfs://namenode:9000/export/vendas.csv"
        
        # Exportar para CSV
        df_vendas_export.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(export_path)
        
        print(f"✅ Dados exportados com sucesso para: {export_path}")
        
        # Verificar se o arquivo foi criado
        try:
            import subprocess
            result = subprocess.run(['hdfs', 'dfs', '-ls', '/export/'], 
                                  capture_output=True, text=True)
            print("\n📁 Arquivos no diretório /export/:")
            print(result.stdout)
        except Exception as e:
            print(f"⚠️ Não foi possível listar arquivos: {e}")
        
        # Verificar conteúdo do arquivo exportado
        print("\n🔍 Verificando dados exportados:")
        df_verificacao = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(export_path)
        
        df_verificacao.show()
        print(f"📈 Total de registros exportados: {df_verificacao.count()}")
        
        print("🎉 Exercício 18 concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro no exercício 18: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()