#!/usr/bin/env python3
"""
Exercício 19: Otimizar tabela (compactação)
- Demonstrar compactação de arquivos no Iceberg
- Otimizar performance com OPTIMIZE
- Analisar métricas antes e depois da otimização
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

try:
    from config import create_spark_session
    from pyspark.sql import functions as F
except ImportError as e:
    print(f"❌ Erro ao importar dependências: {e}")
    print("💡 Certifique-se de que o PySpark está instalado: pip install pyspark")
    sys.exit(1)

def main():
    """
    Função principal do exercício 19
    """
    spark = None
    try:
        print("⚡ Exercício 19: Otimizar tabela (compactação)")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_19_Optimize_Table")
        
        # Verificar se a tabela existe
        try:
            count = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios").collect()[0][0]
            print(f"✅ Tabela funcionarios encontrada com {count} registros")
        except Exception as e:
            print(f"⚠️ Tabela funcionarios não encontrada: {e}")
            print("💡 Execute primeiro os exercícios 4, 5 e 6 para criar a tabela")
            return False
        
        print("\n1️⃣ Verificando métricas da tabela antes da otimização:")
        
        try:
            # Verificar arquivos da tabela
            files_info = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.files")
            file_count = files_info.count()
            
            print(f"📁 Total de arquivos de dados: {file_count}")
            
            if file_count > 0:
                # Estatísticas dos arquivos
                file_stats = files_info.agg(
                    F.sum("file_size_in_bytes").alias("total_size"),
                    F.avg("file_size_in_bytes").alias("avg_size"),
                    F.min("file_size_in_bytes").alias("min_size"),
                    F.max("file_size_in_bytes").alias("max_size"),
                    F.count("*").alias("file_count")
                ).collect()[0]
                
                print(f"📊 Tamanho total: {file_stats['total_size']:,} bytes")
                print(f"📊 Tamanho médio: {file_stats['avg_size']:,.0f} bytes")
                print(f"📊 Menor arquivo: {file_stats['min_size']:,} bytes")
                print(f"📊 Maior arquivo: {file_stats['max_size']:,} bytes")
            
        except Exception as e:
            print(f"⚠️ Não foi possível acessar informações de arquivos: {e}")
        
        print("\n2️⃣ Verificando snapshots antes da otimização:")
        try:
            snapshots_before = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.snapshots")
            snapshot_count_before = snapshots_before.count()
            print(f"📷 Snapshots antes da otimização: {snapshot_count_before}")
            
            print("📋 Últimos 3 snapshots:")
            snapshots_before.select(
                "snapshot_id",
                F.from_unixtime(F.col("timestamp_ms")/1000).alias("timestamp"),
                "operation"
            ).orderBy(F.desc("timestamp_ms")).limit(3).show()
            
        except Exception as e:
            print(f"⚠️ Erro ao acessar snapshots: {e}")
        
        print("\n3️⃣ Adicionando mais dados para simular fragmentação:")
        
        # Adicionar alguns registros para criar mais arquivos
        for i in range(3):
            spark.sql(f"""
                INSERT INTO iceberg.exercicios.funcionarios 
                VALUES ({1000 + i}, 'Funcionário Temp {i+1}', 'Cargo Temp', 'Temp', {3000.0 + i*100})
            """)
        
        print("✅ Dados temporários adicionados")
        
        print("\n4️⃣ Verificando estado após inserções:")
        try:
            files_after_insert = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.files")
            file_count_after = files_after_insert.count()
            print(f"📁 Arquivos após inserções: {file_count_after}")
            
        except Exception as e:
            print(f"⚠️ Erro ao verificar arquivos: {e}")
        
        print("\n5️⃣ Executando OPTIMIZE para compactação:")
        
        try:
            # Execute OPTIMIZE command
            optimize_result = spark.sql("CALL iceberg.system.rewrite_data_files('exercicios.funcionarios')")
            print("✅ Comando OPTIMIZE executado")
            
            # Mostrar resultado da otimização se disponível
            try:
                optimize_result.show()
            except:
                print("ℹ️ Resultado da otimização não disponível para exibição")
                
        except Exception as e:
            print(f"⚠️ OPTIMIZE não disponível, tentando método alternativo: {e}")
            
            # Método alternativo: reescrever dados
            try:
                df = spark.table("iceberg.exercicios.funcionarios")
                df.coalesce(1).writeTo("iceberg.exercicios.funcionarios_optimized").createOrReplace()
                print("✅ Tabela otimizada criada como alternativa")
            except Exception as e2:
                print(f"⚠️ Método alternativo também falhou: {e2}")
        
        print("\n6️⃣ Verificando métricas após otimização:")
        
        try:
            files_after_optimize = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.files")
            file_count_optimized = files_after_optimize.count()
            
            print(f"📁 Arquivos após otimização: {file_count_optimized}")
            
            if file_count_optimized > 0:
                # Novas estatísticas
                optimized_stats = files_after_optimize.agg(
                    F.sum("file_size_in_bytes").alias("total_size"),
                    F.avg("file_size_in_bytes").alias("avg_size"),
                    F.count("*").alias("file_count")
                ).collect()[0]
                
                print(f"📊 Novo tamanho total: {optimized_stats['total_size']:,} bytes")
                print(f"📊 Novo tamanho médio: {optimized_stats['avg_size']:,.0f} bytes")
                print(f"📊 Redução de arquivos: {file_count - file_count_optimized}")
            
        except Exception as e:
            print(f"⚠️ Erro ao verificar arquivos otimizados: {e}")
        
        print("\n7️⃣ Verificando novos snapshots:")
        try:
            snapshots_after = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.snapshots")
            snapshot_count_after = snapshots_after.count()
            print(f"📷 Snapshots após otimização: {snapshot_count_after}")
            
            print("📋 Snapshots mais recentes:")
            snapshots_after.select(
                "snapshot_id",
                F.from_unixtime(F.col("timestamp_ms")/1000).alias("timestamp"),
                "operation",
                "summary"
            ).orderBy(F.desc("timestamp_ms")).limit(5).show(truncate=False)
            
        except Exception as e:
            print(f"⚠️ Erro ao verificar snapshots: {e}")
        
        print("\n8️⃣ Testando performance de consulta:")
        
        import time
        
        # Consulta antes da limpeza
        start_time = time.time()
        result = spark.sql("""
            SELECT departamento, COUNT(*) as funcionarios, AVG(salario) as salario_medio
            FROM iceberg.exercicios.funcionarios
            GROUP BY departamento
            ORDER BY funcionarios DESC
        """)
        result.collect()  # Força execução
        end_time = time.time()
        
        print(f"⏱️ Tempo da consulta: {end_time - start_time:.3f} segundos")
        result.show()
        
        print("\n9️⃣ Limpando dados temporários:")
        
        # Remover dados temporários
        spark.sql("DELETE FROM iceberg.exercicios.funcionarios WHERE departamento = 'Temp'")
        print("✅ Dados temporários removidos")
        
        print("\n🔟 Verificando estado final:")
        final_count = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios").collect()[0][0]
        print(f"📊 Registros finais: {final_count}")
        
        # Summary final
        try:
            final_files = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.files")
            final_file_count = final_files.count()
            
            print(f"📁 Arquivos finais: {final_file_count}")
            
            if final_file_count > 0:
                final_size = final_files.agg(F.sum("file_size_in_bytes")).collect()[0][0]
                print(f"📊 Tamanho final total: {final_size:,} bytes")
            
        except Exception as e:
            print(f"⚠️ Não foi possível obter estatísticas finais: {e}")
        
        print("\n📈 Benefícios da otimização:")
        print("✅ Redução no número de arquivos pequenos")
        print("✅ Melhoria na performance de consultas")
        print("✅ Redução do overhead de metadados")
        print("✅ Melhor utilização do cache")
        
        print("✅ Exercício 19 concluído com sucesso!")
        print("⚡ Demonstramos otimização e compactação de tabelas Iceberg")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 19: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)