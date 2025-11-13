#!/usr/bin/env python3
"""
Exercício 14: Demonstrar time travel no Iceberg
- Usar recursos de time travel para acessar versões históricas
- Visualizar snapshots da tabela
- Fazer consultas em pontos específicos no tempo
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

try:
    from config import create_spark_session
    from pyspark.sql import functions as F
    import time
    from datetime import datetime
except ImportError as e:
    print(f"❌ Erro ao importar dependências: {e}")
    print("💡 Certifique-se de que o PySpark está instalado: pip install pyspark")
    sys.exit(1)

def main():
    """
    Função principal do exercício 14
    """
    spark = None
    try:
        print("⏰ Exercício 14: Time Travel no Iceberg")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_14_Time_Travel")
        
        # Verificar se a tabela existe
        try:
            spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios").show()
            print("✅ Tabela funcionarios encontrada")
        except Exception as e:
            print(f"⚠️ Tabela funcionarios não encontrada: {e}")
            print("💡 Execute primeiro os exercícios 4, 5 e 6 para criar a tabela")
            return False
        
        print("\n1️⃣ Visualizando snapshots da tabela:")
        try:
            # Mostrar histórico de snapshots
            snapshots = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.snapshots")
            print("📷 Histórico de snapshots:")
            snapshots.select(
                "snapshot_id", 
                "timestamp_ms",
                "operation",
                "summary"
            ).show(truncate=False)
            
            # Guardar informações dos snapshots para uso posterior
            snapshot_list = snapshots.select("snapshot_id", "timestamp_ms").collect()
            
        except Exception as e:
            print(f"⚠️ Não foi possível acessar snapshots: {e}")
            snapshot_list = []
        
        print("\n2️⃣ Estado atual da tabela:")
        current_data = spark.sql("""
            SELECT id, nome, departamento, cargo, salario 
            FROM iceberg.exercicios.funcionarios 
            ORDER BY id
        """)
        current_data.show()
        current_count = current_data.count()
        print(f"📊 Total atual de registros: {current_count}")
        
        print("\n3️⃣ Fazendo uma modificação para criar novo snapshot:")
        # Adicionar um funcionário temporário para criar um novo snapshot
        current_time = datetime.now()
        print(f"🕐 Timestamp atual: {current_time}")
        
        spark.sql("""
            INSERT INTO iceberg.exercicios.funcionarios 
            VALUES (999, 'Funcionário Temporário', 'Teste', 'Temporário', 1000.0)
        """)
        
        print("✅ Funcionário temporário adicionado")
        
        # Pequena pausa para garantir timestamps diferentes
        time.sleep(2)
        
        print("\n4️⃣ Verificando novo estado:")
        new_data = spark.sql("""
            SELECT id, nome, departamento, cargo, salario 
            FROM iceberg.exercicios.funcionarios 
            ORDER BY id
        """)
        new_count = new_data.count()
        print(f"📊 Total após inserção: {new_count}")
        
        print("\n5️⃣ Atualizando snapshots:")
        try:
            updated_snapshots = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.snapshots")
            print("📷 Snapshots atualizados:")
            updated_snapshots.select(
                "snapshot_id", 
                F.from_unixtime(F.col("timestamp_ms")/1000).alias("timestamp"),
                "operation"
            ).orderBy("timestamp_ms").show()
            
            # Pegar o snapshot mais recente e o anterior
            recent_snapshots = updated_snapshots.select("snapshot_id", "timestamp_ms")\
                                              .orderBy(F.desc("timestamp_ms"))\
                                              .limit(2).collect()
            
        except Exception as e:
            print(f"⚠️ Erro ao acessar snapshots: {e}")
            recent_snapshots = []
        
        if len(recent_snapshots) >= 2:
            latest_snapshot = recent_snapshots[0]["snapshot_id"]
            previous_snapshot = recent_snapshots[1]["snapshot_id"]
            
            print(f"\n6️⃣ Time Travel - consultando snapshot anterior:")
            print(f"📷 Snapshot anterior: {previous_snapshot}")
            
            try:
                # Consultar dados do snapshot anterior
                previous_data = spark.sql(f"""
                    SELECT id, nome, departamento, cargo, salario 
                    FROM iceberg.exercicios.funcionarios
                    VERSION AS OF {previous_snapshot}
                    ORDER BY id
                """)
                
                print("📊 Dados do snapshot anterior:")
                previous_data.show()
                previous_count = previous_data.count()
                print(f"📈 Registros no snapshot anterior: {previous_count}")
                
            except Exception as e:
                print(f"⚠️ Erro ao consultar snapshot anterior: {e}")
                print("💡 Time travel pode não estar disponível neste ambiente")
        
        print("\n7️⃣ Time Travel usando timestamp:")
        try:
            # Calcular timestamp de 5 minutos atrás
            five_minutes_ago = int((current_time.timestamp() - 300) * 1000)
            
            timestamp_query = spark.sql(f"""
                SELECT COUNT(*) as registros
                FROM iceberg.exercicios.funcionarios
                TIMESTAMP AS OF {five_minutes_ago}
            """)
            
            print(f"📊 Registros há 5 minutos:")
            timestamp_query.show()
            
        except Exception as e:
            print(f"⚠️ Time travel por timestamp não disponível: {e}")
        
        print("\n8️⃣ Comparando versões:")
        # Mostrar diferença entre versões
        try:
            current_ids = spark.sql("SELECT id FROM iceberg.exercicios.funcionarios").rdd.map(lambda r: r[0]).collect()
            
            if len(recent_snapshots) >= 2:
                previous_ids = spark.sql(f"""
                    SELECT id FROM iceberg.exercicios.funcionarios
                    VERSION AS OF {previous_snapshot}
                """).rdd.map(lambda r: r[0]).collect()
                
                new_ids = set(current_ids) - set(previous_ids)
                print(f"🆕 IDs adicionados na última versão: {new_ids}")
                
        except Exception as e:
            print(f"⚠️ Erro na comparação: {e}")
        
        print("\n9️⃣ Limpeza - removendo funcionário temporário:")
        spark.sql("DELETE FROM iceberg.exercicios.funcionarios WHERE id = 999")
        print("✅ Funcionário temporário removido")
        
        print("\n🔟 Verificando snapshots finais:")
        try:
            final_snapshots = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.snapshots")
            final_count = final_snapshots.count()
            print(f"📷 Total de snapshots: {final_count}")
            
            print("📊 Últimos 3 snapshots:")
            final_snapshots.select(
                "snapshot_id",
                F.from_unixtime(F.col("timestamp_ms")/1000).alias("timestamp"),
                "operation"
            ).orderBy(F.desc("timestamp_ms")).limit(3).show()
            
        except Exception as e:
            print(f"⚠️ Erro ao verificar snapshots finais: {e}")
        
        print("✅ Exercício 14 concluído com sucesso!")
        print("⏰ Demonstramos recursos de time travel do Iceberg")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 14: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)