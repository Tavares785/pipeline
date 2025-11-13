#!/usr/bin/env python3
"""
Exercício 10: Fazer merge (upsert) de dados na tabela Iceberg
- Usar MERGE SQL para inserir/atualizar registros
- Demonstrar operações upsert
- Trabalhar com novos dados e dados existentes
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

try:
    from config import create_spark_session
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
except ImportError as e:
    print(f"❌ Erro ao importar dependências: {e}")
    print("💡 Certifique-se de que o PySpark está instalado: pip install pyspark")
    sys.exit(1)

def main():
    """
    Função principal do exercício 10
    """
    spark = None
    try:
        print("🔄 Exercício 10: Merge (upsert) na tabela Iceberg")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_10_Merge_Iceberg")
        
        # Verificar se a tabela existe
        try:
            count_inicial = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios").collect()[0][0]
            print(f"✅ Tabela funcionarios encontrada com {count_inicial} registros")
        except Exception as e:
            print(f"⚠️ Tabela funcionarios não encontrada: {e}")
            print("💡 Execute primeiro os exercícios 4, 5 e 6 para criar a tabela")
            return False
        
        print("\n📊 Estado inicial da tabela:")
        spark.sql("""
            SELECT id, nome, departamento, cargo, salario 
            FROM iceberg.exercicios.funcionarios 
            ORDER BY id
        """).show()
        
        print("\n1️⃣ Criando dados para merge:")
        # Definir schema para novos dados
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("nome", StringType(), True),
            StructField("cargo", StringType(), True),
            StructField("departamento", StringType(), True),
            StructField("salario", DoubleType(), True)
        ])
        
        # Dados para merge: alguns existentes (para atualizar) e alguns novos (para inserir)
        novos_dados = [
            (1, "João Silva", "Desenvolvedor Senior", "TI", 8500.0),  # Atualizar João
            (2, "Maria Santos", "Analista de Vendas Senior", "Vendas", 6500.0),  # Atualizar Maria
            (10, "Pedro Costa", "Analista de Dados", "TI", 6800.0),  # Novo funcionário
            (11, "Lucia Ferreira", "Coordenadora de RH", "RH", 7200.0),  # Novo funcionário
            (12, "Roberto Lima", "Especialista em Marketing", "Marketing", 5800.0)  # Novo funcionário
        ]
        
        # Criar DataFrame temporário
        df_novos = spark.createDataFrame(novos_dados, schema)
        
        print("📝 Dados para merge:")
        df_novos.show()
        
        # Criar view temporária para usar no MERGE
        df_novos.createOrReplaceTempView("novos_funcionarios")
        
        print("\n2️⃣ Executando MERGE SQL:")
        # Executar MERGE SQL
        merge_sql = """
        MERGE INTO iceberg.exercicios.funcionarios AS target
        USING novos_funcionarios AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                nome = source.nome,
                cargo = source.cargo,
                departamento = source.departamento,
                salario = source.salario
        WHEN NOT MATCHED THEN
            INSERT (id, nome, cargo, departamento, salario)
            VALUES (source.id, source.nome, source.cargo, source.departamento, source.salario)
        """
        
        spark.sql(merge_sql)
        print("✅ MERGE executado com sucesso!")
        
        print("\n📊 Estado da tabela após MERGE:")
        result = spark.sql("""
            SELECT id, nome, departamento, cargo, salario 
            FROM iceberg.exercicios.funcionarios 
            ORDER BY id
        """)
        result.show()
        
        count_final = result.count()
        print(f"📈 Registros após merge: {count_final}")
        print(f"🆕 Novos registros adicionados: {count_final - count_inicial}")
        
        print("\n3️⃣ Verificando atualizações específicas:")
        # Verificar os registros que foram atualizados
        atualizados = spark.sql("""
            SELECT id, nome, cargo, salario
            FROM iceberg.exercicios.funcionarios 
            WHERE id IN (1, 2)
            ORDER BY id
        """)
        
        print("🔄 Funcionários atualizados:")
        atualizados.show()
        
        print("\n4️⃣ Verificando novos funcionários:")
        # Verificar os novos funcionários inseridos
        novos = spark.sql("""
            SELECT id, nome, departamento, cargo, salario
            FROM iceberg.exercicios.funcionarios 
            WHERE id >= 10
            ORDER BY id
        """)
        
        print("🆕 Novos funcionários inseridos:")
        novos.show()
        
        print("\n5️⃣ Estatísticas finais por departamento:")
        stats = spark.sql("""
            SELECT 
                departamento,
                COUNT(*) as funcionarios,
                AVG(salario) as salario_medio,
                MIN(salario) as salario_minimo,
                MAX(salario) as salario_maximo
            FROM iceberg.exercicios.funcionarios 
            GROUP BY departamento
            ORDER BY funcionarios DESC
        """)
        stats.show()
        
        print("\n6️⃣ Segundo merge para demonstrar idempotência:")
        # Executar o mesmo merge novamente para mostrar que é idempotente
        spark.sql(merge_sql)
        
        count_segundo_merge = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios").collect()[0][0]
        print(f"📊 Registros após segundo merge: {count_segundo_merge}")
        print(f"✅ MERGE é idempotente: {count_segundo_merge == count_final}")
        
        print("✅ Exercício 10 concluído com sucesso!")
        print("🔄 Demonstramos operações MERGE (upsert) na tabela Iceberg")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 10: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)