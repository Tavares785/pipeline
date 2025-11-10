#!/usr/bin/env python3
"""
Configuração comum para todos os exercícios
Este arquivo contém configurações e utilitários compartilhados
"""

def get_spark_iceberg_config():
    """
    Retorna as configurações padrão do Spark com Iceberg
    """
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.type": "hive",
        "spark.sql.catalog.spark_catalog.uri": "thrift://hive-metastore:9083",
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "hive",
        "spark.sql.catalog.iceberg.uri": "thrift://hive-metastore:9083",
        "spark.sql.catalog.iceberg.warehouse": "hdfs://namenode:9000/warehouse"
    }

def create_spark_session(app_name, additional_configs=None):
    """
    Cria uma sessão Spark com configurações Iceberg
    """
    try:
        from pyspark.sql import SparkSession
        
        builder = SparkSession.builder.appName(app_name)
        
        # Adicionar configurações Iceberg
        iceberg_configs = get_spark_iceberg_config()
        for key, value in iceberg_configs.items():
            builder = builder.config(key, value)
        
        # Adicionar configurações adicionais se fornecidas
        if additional_configs:
            for key, value in additional_configs.items():
                builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
        
    except ImportError:
        print("❌ PySpark não está instalado ou não está no PATH")
        print("💡 Para instalar: pip install pyspark")
        return None
    except Exception as e:
        print(f"❌ Erro ao criar sessão Spark: {e}")
        return None

def print_success(message):
    """Imprime mensagem de sucesso formatada"""
    print(f"✅ {message}")

def print_error(message):
    """Imprime mensagem de erro formatada"""
    print(f"❌ {message}")

def print_info(message):
    """Imprime mensagem informativa formatada"""
    print(f"ℹ️ {message}")

def print_warning(message):
    """Imprime mensagem de aviso formatada"""
    print(f"⚠️ {message}")

# Dados de exemplo para os exercícios
DADOS_PESSOAS = [(1, "João"), (2, "Maria"), (3, "Pedro")]

DADOS_VENDAS = [
    (1, 1500.50, 2022),
    (2, 2300.75, 2022),
    (3, 1800.00, 2023),
    (4, 2100.25, 2023),
    (5, 2500.00, 2023),
    (6, 1900.00, 2024),
    (7, 2200.50, 2024)
]

DADOS_PRODUTOS = [
    (1, "Notebook", 2500.00, "Eletrônicos"),
    (2, "Mouse", 50.00, "Eletrônicos"),
    (3, "Teclado", 150.00, "Eletrônicos"),
    (4, "Monitor", 800.00, "Eletrônicos"),
    (5, "Cadeira", 400.00, "Móveis"),
    (6, "Mesa", 600.00, "Móveis")
]