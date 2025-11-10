#!/usr/bin/env python3
"""
Exercício 5: Criar tabela Iceberg
Objetivo: Criar uma tabela Iceberg lab.db.pessoas (id INT, nome STRING) usando SQL.
"""

from pyspark.sql import SparkSession

def main():
    # Criar sessão Spark com configuração Iceberg
    spark = SparkSession.builder \
        .appName("Exercicio05_Tabela_Iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/warehouse") \
        .getOrCreate()
    
    try:
        print("🗃️ Exercício 5: Criando tabela Iceberg...")
        
        # SQL para criar tabela pessoas
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS iceberg.lab.db.pessoas (
            id INT,
            nome STRING
        ) USING ICEBERG
        """
        
        # Executar criação da tabela
        spark.sql(create_table_sql)
        print("✅ Tabela 'lab.db.pessoas' criada com sucesso!")
        
        # Verificar se a tabela foi criada
        print("\n📋 Tabelas no namespace lab.db:")
        try:
            spark.sql("SHOW TABLES IN iceberg.lab.db").show()
        except Exception as e:
            print(f"⚠️ Erro: {e}")
            # Alternativa
            spark.sql("SHOW TABLES").show()
        
        # Mostrar estrutura da tabela
        print("\n📊 Estrutura da tabela pessoas:")
        spark.sql("DESCRIBE iceberg.lab.db.pessoas").show()
        
        print("🎉 Exercício 5 concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro no exercício 5: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()