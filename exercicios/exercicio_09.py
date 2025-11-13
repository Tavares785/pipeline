#!/usr/bin/env python3
"""
Exercício 9: Deletar registros da tabela Iceberg
- Usar DELETE SQL para remover registros
- Demonstrar deleções condicionais
- Verificar impacto nas estatísticas
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
    Função principal do exercício 9
    """
    spark = None
    try:
        print("🗑️ Exercício 9: Deletar registros da tabela Iceberg")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_09_Delete_Iceberg")
        
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
            SELECT nome, departamento, cargo, salario 
            FROM iceberg.exercicios.funcionarios 
            ORDER BY salario DESC
        """).show()
        
        print("\n1️⃣ Primeiro, vamos adicionar um funcionário temporário:")
        # Adicionar funcionário que será deletado
        spark.sql("""
            INSERT INTO iceberg.exercicios.funcionarios 
            VALUES (999, 'João Temporário', 'Estagiário', 'Temporário', 1500)
        """)
        
        print("✅ Funcionário temporário adicionado")
        
        print("\n📊 Verificando inserção:")
        spark.sql("""
            SELECT * FROM iceberg.exercicios.funcionarios 
            WHERE nome LIKE '%Temporário%'
        """).show()
        
        print("\n2️⃣ Deletar funcionário específico por ID:")
        # Deletar o funcionário temporário
        deleted_rows = spark.sql("""
            DELETE FROM iceberg.exercicios.funcionarios 
            WHERE id = 999
        """)
        
        print("✅ Funcionário temporário deletado")
        
        print("\n3️⃣ Deletar funcionários com salário baixo:")
        # Vamos simular a remoção de funcionários com salário muito baixo
        # (cuidado: isso é apenas para demonstração)
        print("⚠️ Simulando deleção de funcionários com salário < 3000")
        
        # Primeiro, mostrar quem seria afetado
        funcionarios_baixo_salario = spark.sql("""
            SELECT nome, departamento, cargo, salario 
            FROM iceberg.exercicios.funcionarios 
            WHERE salario < 3000
        """)
        
        print("👥 Funcionários que seriam afetados:")
        funcionarios_baixo_salario.show()
        
        # Para fins educacionais, não vamos realmente deletar
        print("💡 Por fins educacionais, não deletaremos estes registros")
        
        print("\n4️⃣ Deletar por departamento (simulação):")
        # Mostrar como deletar por departamento
        temp_count = spark.sql("""
            SELECT COUNT(*) as total 
            FROM iceberg.exercicios.funcionarios 
            WHERE departamento = 'Temporário'
        """).collect()[0][0]
        
        if temp_count > 0:
            spark.sql("""
                DELETE FROM iceberg.exercicios.funcionarios 
                WHERE departamento = 'Temporário'
            """)
            print("✅ Registros do departamento 'Temporário' removidos")
        else:
            print("ℹ️ Nenhum registro encontrado no departamento 'Temporário'")
        
        print("\n📊 Estado final da tabela:")
        result = spark.sql("""
            SELECT nome, departamento, cargo, salario 
            FROM iceberg.exercicios.funcionarios 
            ORDER BY salario DESC
        """)
        result.show()
        
        count_final = result.count()
        print(f"📈 Registros finais: {count_final}")
        print(f"🔄 Diferença: {count_inicial - count_final} registros removidos")
        
        print("\n5️⃣ Estatísticas após deleções:")
        stats = spark.sql("""
            SELECT 
                COUNT(*) as total_funcionarios,
                AVG(salario) as salario_medio,
                MIN(salario) as salario_minimo,
                MAX(salario) as salario_maximo
            FROM iceberg.exercicios.funcionarios
        """)
        stats.show()
        
        print("\n6️⃣ Contagem por departamento:")
        por_depto = spark.sql("""
            SELECT 
                departamento,
                COUNT(*) as funcionarios
            FROM iceberg.exercicios.funcionarios 
            GROUP BY departamento
            ORDER BY funcionarios DESC
        """)
        por_depto.show()
        
        print("✅ Exercício 9 concluído com sucesso!")
        print("🗑️ Demonstramos operações DELETE na tabela Iceberg")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 9: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)