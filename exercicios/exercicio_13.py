#!/usr/bin/env python3
"""
Exercício 13: Consultar tabela particionada
- Fazer consultas eficientes na tabela particionada
- Demonstrar partition pruning
- Comparar performance com e sem filtros de partição
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
    Função principal do exercício 13
    """
    spark = None
    try:
        print("🔍 Exercício 13: Consultar tabela particionada")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_13_Query_Partitioned")
        
        # Verificar se a tabela particionada existe
        try:
            count = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios_por_depto").collect()[0][0]
            print(f"✅ Tabela funcionarios_por_depto encontrada com {count} registros")
        except Exception as e:
            print(f"⚠️ Tabela funcionarios_por_depto não encontrada: {e}")
            print("💡 Execute primeiro os exercícios 11 e 12 para criar e popular a tabela")
            return False
        
        print("\n1️⃣ Consulta com partition pruning - departamento específico:")
        # Esta consulta é eficiente porque usa a coluna de partição
        ti_query = spark.sql("""
            SELECT nome, cargo, salario
            FROM iceberg.exercicios.funcionarios_por_depto
            WHERE departamento = 'TI'
            ORDER BY salario DESC
        """)
        
        print("👥 Funcionários do departamento de TI:")
        ti_query.show()
        
        # Mostrar plano de execução (se disponível)
        print("\n📋 Plano de execução da consulta:")
        try:
            ti_query.explain()
        except Exception as e:
            print(f"⚠️ Não foi possível mostrar o plano: {e}")
        
        print("\n2️⃣ Consulta com múltiplas partições:")
        # Consultar múltiplos departamentos
        multi_dept = spark.sql("""
            SELECT departamento, nome, cargo, salario
            FROM iceberg.exercicios.funcionarios_por_depto
            WHERE departamento IN ('TI', 'Vendas', 'Marketing')
            ORDER BY departamento, salario DESC
        """)
        
        print("👥 Funcionários de TI, Vendas e Marketing:")
        multi_dept.show()
        
        print("\n3️⃣ Consulta sem filtro de partição:")
        # Esta consulta lê todas as partições
        high_salaries = spark.sql("""
            SELECT nome, departamento, cargo, salario
            FROM iceberg.exercicios.funcionarios_por_depto
            WHERE salario > 8000
            ORDER BY salario DESC
        """)
        
        print("💰 Funcionários com salário > R$ 8000:")
        high_salaries.show()
        
        print("\n4️⃣ Agregações por partição:")
        # Estatísticas por departamento (muito eficiente)
        stats_by_dept = spark.sql("""
            SELECT 
                departamento,
                COUNT(*) as total_funcionarios,
                AVG(salario) as salario_medio,
                MIN(salario) as salario_minimo,
                MAX(salario) as salario_maximo,
                STDDEV(salario) as desvio_padrao
            FROM iceberg.exercicios.funcionarios_por_depto
            GROUP BY departamento
            ORDER BY salario_medio DESC
        """)
        
        print("📊 Estatísticas por departamento:")
        stats_by_dept.show()
        
        print("\n5️⃣ Consulta complexa com join interno:")
        # Simular join com outra tabela (usando a própria tabela para demonstração)
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW dept_summary AS
            SELECT 
                departamento,
                AVG(salario) as salario_medio_dept
            FROM iceberg.exercicios.funcionarios_por_depto
            GROUP BY departamento
        """)
        
        complex_query = spark.sql("""
            SELECT 
                f.nome,
                f.departamento,
                f.cargo,
                f.salario,
                d.salario_medio_dept,
                CASE 
                    WHEN f.salario > d.salario_medio_dept THEN 'Acima da Média'
                    ELSE 'Abaixo da Média'
                END as posicao_salarial
            FROM iceberg.exercicios.funcionarios_por_depto f
            JOIN dept_summary d ON f.departamento = d.departamento
            WHERE f.departamento = 'TI'
            ORDER BY f.salario DESC
        """)
        
        print("🎯 Análise salarial do departamento de TI:")
        complex_query.show()
        
        print("\n6️⃣ Window functions com partição:")
        # Usar window functions para ranking
        ranking_query = spark.sql("""
            SELECT 
                nome,
                departamento,
                cargo,
                salario,
                ROW_NUMBER() OVER (PARTITION BY departamento ORDER BY salario DESC) as ranking_dept,
                DENSE_RANK() OVER (ORDER BY salario DESC) as ranking_geral
            FROM iceberg.exercicios.funcionarios_por_depto
        """)
        
        print("🏆 Ranking salarial por departamento:")
        ranking_query.show()
        
        print("\n7️⃣ Consulta de performance - contagem por departamento:")
        # Esta é muito rápida devido ao particionamento
        import time
        start_time = time.time()
        
        count_by_dept = spark.sql("""
            SELECT departamento, COUNT(*) as funcionarios
            FROM iceberg.exercicios.funcionarios_por_depto
            GROUP BY departamento
            ORDER BY funcionarios DESC
        """)
        
        count_by_dept.show()
        end_time = time.time()
        
        print(f"⏱️ Tempo de execução: {end_time - start_time:.3f} segundos")
        
        print("\n8️⃣ Filtros avançados com múltiplas condições:")
        advanced_filter = spark.sql("""
            SELECT 
                departamento,
                nome,
                cargo,
                salario
            FROM iceberg.exercicios.funcionarios_por_depto
            WHERE departamento = 'TI' 
            AND salario BETWEEN 6000 AND 9000
            AND cargo LIKE '%Senior%' OR cargo LIKE '%Lead%'
            ORDER BY salario DESC
        """)
        
        print("🎯 Filtros avançados - TI com salário entre 6k-9k e cargo senior/lead:")
        advanced_filter.show()
        
        print("✅ Exercício 13 concluído com sucesso!")
        print("🔍 Demonstramos consultas eficientes em tabela particionada")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 13: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)