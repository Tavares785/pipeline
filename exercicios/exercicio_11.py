#!/usr/bin/env python3
"""
Exercício 11: Criar tabela particionada
Objetivo: Criar uma tabela Iceberg com partição por ano.
"""

from pyspark.sql import SparkSession

def main():
    # Criar sessão Spark com configuração Iceberg
    spark = SparkSession.builder \
        .appName("Exercicio11_Tabela_Particionada") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
        .getOrCreate()
    
    try:
        print("🗂️ Exercício 11: Criando tabela particionada...")
        
        # SQL para criar tabela particionada por ano
        create_vendas_sql = """
        CREATE TABLE IF NOT EXISTS iceberg.lab.db.vendas (
            id INT,
            valor DOUBLE,
            ano INT
        ) USING ICEBERG
        PARTITIONED BY (ano)
        """
        
        # Executar criação da tabela
        spark.sql(create_vendas_sql)
        print("✅ Tabela 'vendas' particionada criada com sucesso!")
        
        # Verificar estrutura da tabela
        print("\n📊 Estrutura da tabela vendas:")
        spark.sql("DESCRIBE iceberg.lab.db.vendas").show()
        
        # Mostrar informações de particionamento
        print("\n🗂️ Informações da tabela:")
        try:
            spark.sql("DESCRIBE DETAIL iceberg.lab.db.vendas").show()
        except Exception as e:
            print(f"⚠️ Não foi possível mostrar detalhes: {e}")
        
        print("🎉 Exercício 11 concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro no exercício 11: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()