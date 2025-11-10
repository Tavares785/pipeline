#!/usr/bin/env python3
"""
Exercício 20: Analytics com agregações complexas
- Demonstrar análises avançadas de dados
- Usar window functions, rollups e cube
- Criar relatórios analíticos completos
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

try:
    from config import create_spark_session
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
except ImportError as e:
    print(f"❌ Erro ao importar dependências: {e}")
    print("💡 Certifique-se de que o PySpark está instalado: pip install pyspark")
    sys.exit(1)

def main():
    """
    Função principal do exercício 20
    """
    spark = None
    try:
        print("📊 Exercício 20: Analytics com agregações complexas")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_20_Advanced_Analytics")
        
        # Verificar se as tabelas existem
        try:
            func_count = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios").collect()[0][0]
            dept_count = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.departamentos").collect()[0][0]
            print(f"✅ Tabela funcionarios: {func_count} registros")
            print(f"✅ Tabela departamentos: {dept_count} registros")
        except Exception as e:
            print(f"⚠️ Tabelas não encontradas: {e}")
            print("💡 Execute primeiro os exercícios 4-6 e 16 para criar as tabelas")
            return False
        
        print("\n1️⃣ Análise de distribuição salarial com percentis:")
        
        percentile_analysis = spark.sql("""
            SELECT 
                departamento,
                COUNT(*) as funcionarios,
                MIN(salario) as salario_min,
                PERCENTILE_APPROX(salario, 0.25) as percentil_25,
                PERCENTILE_APPROX(salario, 0.5) as mediana,
                AVG(salario) as media,
                PERCENTILE_APPROX(salario, 0.75) as percentil_75,
                MAX(salario) as salario_max,
                STDDEV(salario) as desvio_padrao,
                VAR_SAMP(salario) as variancia
            FROM iceberg.exercicios.funcionarios
            GROUP BY departamento
            ORDER BY media DESC
        """)
        
        print("📊 Análise estatística por departamento:")
        percentile_analysis.show()
        
        print("\n2️⃣ Window functions - ranking e comparações:")
        
        window_analysis = spark.sql("""
            SELECT 
                nome,
                departamento,
                cargo,
                salario,
                -- Rankings
                ROW_NUMBER() OVER (ORDER BY salario DESC) as ranking_geral,
                DENSE_RANK() OVER (PARTITION BY departamento ORDER BY salario DESC) as rank_departamento,
                PERCENT_RANK() OVER (ORDER BY salario) as percentil_salarial,
                
                -- Comparações com valores adjacentes
                LAG(salario, 1) OVER (PARTITION BY departamento ORDER BY salario) as salario_anterior,
                LEAD(salario, 1) OVER (PARTITION BY departamento ORDER BY salario) as proximo_salario,
                
                -- Estatísticas móveis
                AVG(salario) OVER (PARTITION BY departamento) as media_departamento,
                AVG(salario) OVER (ORDER BY salario ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as media_movel_5,
                
                -- Diferenças
                salario - AVG(salario) OVER (PARTITION BY departamento) as diferenca_media_dept,
                salario - LAG(salario, 1) OVER (PARTITION BY departamento ORDER BY salario) as diferenca_anterior
            FROM iceberg.exercicios.funcionarios
        """)
        
        print("🏆 Análise com window functions:")
        window_analysis.show()
        
        print("\n3️⃣ ROLLUP - agregações hierárquicas:")
        
        rollup_analysis = spark.sql("""
            SELECT 
                departamento,
                cargo,
                COUNT(*) as funcionarios,
                SUM(salario) as folha_salarial,
                AVG(salario) as salario_medio,
                MIN(salario) as salario_min,
                MAX(salario) as salario_max
            FROM iceberg.exercicios.funcionarios
            GROUP BY ROLLUP(departamento, cargo)
            ORDER BY departamento NULLS LAST, cargo NULLS LAST
        """)
        
        print("📊 Análise ROLLUP (hierárquica):")
        rollup_analysis.show()
        
        print("\n4️⃣ CUBE - todas as combinações possíveis:")
        
        cube_analysis = spark.sql("""
            SELECT 
                departamento,
                CASE 
                    WHEN salario < 5000 THEN 'Baixo'
                    WHEN salario < 8000 THEN 'Médio'
                    ELSE 'Alto'
                END as faixa_salarial,
                COUNT(*) as funcionarios,
                AVG(salario) as salario_medio
            FROM iceberg.exercicios.funcionarios
            GROUP BY CUBE(departamento, 
                         CASE 
                             WHEN salario < 5000 THEN 'Baixo'
                             WHEN salario < 8000 THEN 'Médio'
                             ELSE 'Alto'
                         END)
            ORDER BY departamento NULLS LAST, faixa_salarial NULLS LAST
        """)
        
        print("📊 Análise CUBE (todas combinações):")
        cube_analysis.show()
        
        print("\n5️⃣ Análise temporal simulada (usando IDs como proxy):")
        
        temporal_analysis = spark.sql("""
            WITH funcionarios_com_periodo AS (
                SELECT *,
                    CASE 
                        WHEN id <= 3 THEN 'Q1-2024'
                        WHEN id <= 6 THEN 'Q2-2024'
                        WHEN id <= 10 THEN 'Q3-2024'
                        ELSE 'Q4-2024'
                    END as periodo_contratacao
                FROM iceberg.exercicios.funcionarios
            )
            SELECT 
                periodo_contratacao,
                departamento,
                COUNT(*) as contratacoes,
                AVG(salario) as salario_medio_periodo,
                SUM(COUNT(*)) OVER (PARTITION BY departamento ORDER BY periodo_contratacao) as acumulado_dept,
                LAG(COUNT(*)) OVER (PARTITION BY departamento ORDER BY periodo_contratacao) as periodo_anterior
            FROM funcionarios_com_periodo
            GROUP BY periodo_contratacao, departamento
            ORDER BY departamento, periodo_contratacao
        """)
        
        print("📅 Análise temporal por período:")
        temporal_analysis.show()
        
        print("\n6️⃣ Análise de correlação departamento-orçamento:")
        
        correlation_analysis = spark.sql("""
            SELECT 
                d.nome as departamento,
                d.orcamento,
                d.localizacao,
                COUNT(f.id) as funcionarios,
                COALESCE(SUM(f.salario), 0) as folha_atual,
                d.orcamento - COALESCE(SUM(f.salario), 0) as saldo_orcamento,
                ROUND((COALESCE(SUM(f.salario), 0) / d.orcamento) * 100, 2) as utilizacao_percentual,
                CASE 
                    WHEN COUNT(f.id) = 0 THEN 'Sem funcionários'
                    WHEN (COALESCE(SUM(f.salario), 0) / d.orcamento) > 0.9 THEN 'Orçamento quase esgotado'
                    WHEN (COALESCE(SUM(f.salario), 0) / d.orcamento) > 0.7 THEN 'Uso adequado'
                    ELSE 'Subutilizado'
                END as status_orcamentario
            FROM iceberg.exercicios.departamentos d
            LEFT JOIN iceberg.exercicios.funcionarios f ON d.nome = f.departamento
            GROUP BY d.nome, d.orcamento, d.localizacao
        """)
        
        print("💰 Análise orçamento vs realizado:")
        correlation_analysis.show()
        
        print("\n7️⃣ Top performers e análise de outliers:")
        
        outlier_analysis = spark.sql("""
            WITH stats_dept AS (
                SELECT 
                    departamento,
                    AVG(salario) as media,
                    STDDEV(salario) as desvio
                FROM iceberg.exercicios.funcionarios
                GROUP BY departamento
            )
            SELECT 
                f.nome,
                f.departamento,
                f.cargo,
                f.salario,
                s.media as media_dept,
                s.desvio as desvio_dept,
                ROUND((f.salario - s.media) / s.desvio, 2) as z_score,
                CASE 
                    WHEN ABS((f.salario - s.media) / s.desvio) > 2 THEN 'Outlier'
                    WHEN (f.salario - s.media) / s.desvio > 1.5 THEN 'Alto'
                    WHEN (f.salario - s.media) / s.desvio < -1.5 THEN 'Baixo'
                    ELSE 'Normal'
                END as classificacao
            FROM iceberg.exercicios.funcionarios f
            JOIN stats_dept s ON f.departamento = s.departamento
            ORDER BY ABS((f.salario - s.media) / s.desvio) DESC
        """)
        
        print("🎯 Análise de outliers salariais:")
        outlier_analysis.show()
        
        print("\n8️⃣ Dashboard executivo - KPIs principais:")
        
        executive_dashboard = spark.sql("""
            WITH kpis AS (
                SELECT 
                    COUNT(DISTINCT f.departamento) as total_departamentos,
                    COUNT(*) as total_funcionarios,
                    AVG(f.salario) as salario_medio_empresa,
                    SUM(f.salario) as folha_total_empresa,
                    SUM(d.orcamento) as orcamento_total,
                    MAX(f.salario) as maior_salario,
                    MIN(f.salario) as menor_salario,
                    COUNT(DISTINCT f.cargo) as total_cargos
                FROM iceberg.exercicios.funcionarios f
                LEFT JOIN iceberg.exercicios.departamentos d ON f.departamento = d.nome
            )
            SELECT 
                'EMPRESA OVERVIEW' as categoria,
                CONCAT('Total Funcionários: ', total_funcionarios) as metrica1,
                CONCAT('Departamentos: ', total_departamentos) as metrica2,
                CONCAT('Cargos Únicos: ', total_cargos) as metrica3,
                CONCAT('Salário Médio: R$ ', ROUND(salario_medio_empresa, 2)) as metrica4,
                CONCAT('Folha Total: R$ ', ROUND(folha_total_empresa, 2)) as metrica5,
                CONCAT('Orçamento Total: R$ ', ROUND(orcamento_total, 2)) as metrica6,
                CONCAT('Utilização: ', ROUND((folha_total_empresa/orcamento_total)*100, 1), '%') as metrica7
            FROM kpis
        """)
        
        print("📊 Dashboard Executivo:")
        executive_dashboard.show(truncate=False)
        
        print("\n9️⃣ Matriz de correlação simplificada:")
        
        correlation_matrix = spark.sql("""
            SELECT 
                f1.departamento as dept1,
                f2.departamento as dept2,
                CORR(f1.salario, f2.salario) as correlacao_salarial,
                COUNT(*) as pares_comparados
            FROM iceberg.exercicios.funcionarios f1
            CROSS JOIN iceberg.exercicios.funcionarios f2
            WHERE f1.departamento != f2.departamento
            GROUP BY f1.departamento, f2.departamento
            HAVING COUNT(*) >= 2
            ORDER BY correlacao_salarial DESC
        """)
        
        print("🔗 Matriz de correlação entre departamentos:")
        correlation_matrix.show()
        
        print("\n🔟 Relatório final consolidado:")
        
        final_report = spark.sql("""
            SELECT 
                'RESUMO ANALÍTICO' as secao,
                CONCAT('Maior salário: R$ ', MAX(salario)) as dado1,
                CONCAT('Menor salário: R$ ', MIN(salario)) as dado2,
                CONCAT('Amplitude salarial: R$ ', MAX(salario) - MIN(salario)) as dado3,
                CONCAT('Funcionários acima média: ', 
                       SUM(CASE WHEN salario > AVG(salario) OVER() THEN 1 ELSE 0 END)) as dado4,
                CONCAT('Departamento com maior folha: ', 
                       FIRST(departamento) OVER (ORDER BY SUM(salario) OVER (PARTITION BY departamento) DESC)) as dado5
            FROM iceberg.exercicios.funcionarios
            LIMIT 1
        """)
        
        print("📋 Relatório Final:")
        final_report.show(truncate=False)
        
        print("✅ Exercício 20 concluído com sucesso!")
        print("📊 Demonstramos análises avançadas e agregações complexas")
        print("\n🎉 PARABÉNS! Você completou todos os 20 exercícios de Big Data!")
        print("🚀 Agora você domina Spark + Iceberg + HDFS para análises de Big Data!")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 20: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)