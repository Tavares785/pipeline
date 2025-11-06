#!/usr/bin/env python3
"""
Teste simples de PySpark - só criação de sessão
"""

import sys
import os

try:
    from pyspark.sql import SparkSession
    print("✅ PySpark importado com sucesso!")
    
    # Criar sessão Spark com configurações simples para Windows
    spark = SparkSession.builder \
        .appName("Teste_Simples") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    print("✅ Spark Session criada com sucesso!")
    print(f"🎯 Versão do Spark: {spark.version}")
    print(f"🔧 Configuração do Spark: {spark.conf.get('spark.app.name')}")
    
    # Teste muito simples - sem operações que requerem workers
    try:
        # Criar range simples
        df = spark.range(5)
        print("✅ DataFrame range criado")
        
        # Coletar sem usar show() que pode ter problemas
        data = df.collect()
        print(f"📊 Dados coletados: {[row.id for row in data]}")
        
    except Exception as e:
        print(f"⚠️ Erro na operação: {e}")
    
    spark.stop()
    print("✅ Teste concluído com sucesso!")
    
except ImportError as e:
    print(f"❌ Erro ao importar PySpark: {e}")
except Exception as e:
    print(f"❌ Erro geral: {e}")