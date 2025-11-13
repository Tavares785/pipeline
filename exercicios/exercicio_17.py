#!/usr/bin/env python3
"""
Exercício 17: Criar view temporária e SQL complexo
- Criar views temporárias a partir de tabelas Iceberg
- Demonstrar consultas SQL complexas
- Usar CTEs (Common Table Expressions)
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
    Função principal do exercício 17
    """
    spark = None
    try:
        print("👁️ Exercício 17: Views temporárias e SQL complexo")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_17_Views_Complex_SQL")
        
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
        
        print("\n1️⃣ Criando view temporária - estatísticas por departamento:")
        
        # View com estatísticas agregadas
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW vw_stats_departamento AS
            SELECT 
                f.departamento,
                COUNT(*) as total_funcionarios,
                AVG(f.salario) as salario_medio,
                MIN(f.salario) as salario_minimo,
                MAX(f.salario) as salario_maximo,
                SUM(f.salario) as folha_salarial,
                STDDEV(f.salario) as desvio_padrao,
                d.orcamento,
                d.gerente,
                d.localizacao
            FROM iceberg.exercicios.funcionarios f
            LEFT JOIN iceberg.exercicios.departamentos d 
                ON f.departamento = d.nome
            GROUP BY f.departamento, d.orcamento, d.gerente, d.localizacao
        """)
        
        print("✅ View vw_stats_departamento criada")
        
        print("\n📊 Consultando view de estatísticas:")
        spark.sql("""
            SELECT 
                departamento,
                total_funcionarios,
                ROUND(salario_medio, 2) as salario_medio,
                ROUND(folha_salarial, 2) as folha_salarial,
                ROUND(orcamento, 2) as orcamento,
                ROUND(orcamento - folha_salarial, 2) as saldo_orcamento
            FROM vw_stats_departamento
            ORDER BY total_funcionarios DESC
        """).show()
        
        print("\n2️⃣ View temporária - funcionários acima da média:")
        
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW vw_funcionarios_destaque AS
            SELECT 
                f.nome,
                f.cargo,
                f.departamento,
                f.salario,
                s.salario_medio as media_departamento,
                ROUND(f.salario - s.salario_medio, 2) as diferenca_media,
                CASE 
                    WHEN f.salario > s.salario_medio * 1.5 THEN 'Muito Alto'
                    WHEN f.salario > s.salario_medio * 1.2 THEN 'Alto'
                    WHEN f.salario > s.salario_medio THEN 'Acima da Média'
                    ELSE 'Abaixo da Média'
                END as classificacao_salarial
            FROM iceberg.exercicios.funcionarios f
            JOIN vw_stats_departamento s ON f.departamento = s.departamento
        """)
        
        print("✅ View vw_funcionarios_destaque criada")
        
        print("\n📊 Funcionários com destaque salarial:")
        spark.sql("""
            SELECT nome, cargo, departamento, salario, 
                   ROUND(media_departamento, 2) as media_dept,
                   diferenca_media, classificacao_salarial
            FROM vw_funcionarios_destaque
            WHERE classificacao_salarial != 'Abaixo da Média'
            ORDER BY diferenca_media DESC
        """).show()
        
        print("\n3️⃣ CTE (Common Table Expression) - análise hierárquica:")
        
        hierarchical_analysis = spark.sql("""
            WITH salario_ranges AS (
                SELECT 
                    CASE 
                        WHEN salario < 4000 THEN 'Júnior (< 4k)'
                        WHEN salario < 7000 THEN 'Pleno (4k-7k)'
                        WHEN salario < 10000 THEN 'Sênior (7k-10k)'
                        ELSE 'Especialista (> 10k)'
                    END as faixa_salarial,
                    departamento,
                    COUNT(*) as funcionarios,
                    AVG(salario) as salario_medio_faixa
                FROM iceberg.exercicios.funcionarios
                GROUP BY 1, 2
            ),
            department_totals AS (
                SELECT 
                    departamento,
                    SUM(funcionarios) as total_dept
                FROM salario_ranges
                GROUP BY departamento
            )
            SELECT 
                sr.departamento,
                sr.faixa_salarial,
                sr.funcionarios,
                ROUND(sr.salario_medio_faixa, 2) as salario_medio_faixa,
                dt.total_dept,
                ROUND((sr.funcionarios * 100.0 / dt.total_dept), 1) as percentual_faixa
            FROM salario_ranges sr
            JOIN department_totals dt ON sr.departamento = dt.departamento
            ORDER BY sr.departamento, sr.salario_medio_faixa DESC
        """)
        
        print("📊 Análise por faixas salariais:")
        hierarchical_analysis.show()
        
        print("\n4️⃣ CTE complexo - análise de performance departamental:")
        
        performance_analysis = spark.sql("""
            WITH dept_metrics AS (
                SELECT 
                    f.departamento,
                    COUNT(*) as funcionarios,
                    AVG(f.salario) as salario_medio,
                    SUM(f.salario) as folha_total,
                    d.orcamento,
                    d.gerente
                FROM iceberg.exercicios.funcionarios f
                LEFT JOIN iceberg.exercicios.departamentos d ON f.departamento = d.nome
                GROUP BY f.departamento, d.orcamento, d.gerente
            ),
            efficiency_calc AS (
                SELECT 
                    departamento,
                    funcionarios,
                    salario_medio,
                    folha_total,
                    orcamento,
                    gerente,
                    ROUND((folha_total / orcamento) * 100, 2) as utilizacao_orcamento,
                    ROUND(folha_total / funcionarios, 2) as custo_por_funcionario,
                    CASE 
                        WHEN (folha_total / orcamento) > 0.9 THEN 'Alto Uso'
                        WHEN (folha_total / orcamento) > 0.7 THEN 'Uso Moderado'
                        ELSE 'Baixo Uso'
                    END as status_orcamento
                FROM dept_metrics
                WHERE orcamento IS NOT NULL
            )
            SELECT 
                departamento,
                gerente,
                funcionarios,
                ROUND(salario_medio, 2) as salario_medio,
                ROUND(folha_total, 2) as folha_total,
                ROUND(orcamento, 2) as orcamento,
                utilizacao_orcamento,
                custo_por_funcionario,
                status_orcamento
            FROM efficiency_calc
            ORDER BY utilizacao_orcamento DESC
        """)
        
        print("📊 Análise de eficiência departamental:")
        performance_analysis.show()
        
        print("\n5️⃣ View materializada (simulada) - ranking geral:")
        
        # Criar uma view complexa que simula materialização
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW vw_ranking_geral AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY salario DESC) as posicao_geral,
                nome,
                cargo,
                departamento,
                salario,
                DENSE_RANK() OVER (PARTITION BY departamento ORDER BY salario DESC) as posicao_departamento,
                PERCENT_RANK() OVER (ORDER BY salario) as percentil_salarial,
                LAG(salario) OVER (ORDER BY salario DESC) as salario_anterior,
                LEAD(salario) OVER (ORDER BY salario DESC) as proximo_salario
            FROM iceberg.exercicios.funcionarios
        """)
        
        print("📊 Ranking geral com window functions:")
        spark.sql("""
            SELECT 
                posicao_geral,
                nome,
                departamento,
                cargo,
                salario,
                posicao_departamento,
                ROUND(percentil_salarial * 100, 1) as percentil
            FROM vw_ranking_geral
            WHERE posicao_geral <= 10
            ORDER BY posicao_geral
        """).show()
        
        print("\n6️⃣ Consulta recursiva simulada - estrutura organizacional:")
        
        # Simular uma estrutura hierárquica
        org_structure = spark.sql("""
            WITH org_hierarchy AS (
                SELECT 
                    d.nome as departamento,
                    d.gerente,
                    COUNT(f.id) as subordinados_diretos,
                    AVG(f.salario) as salario_medio_equipe,
                    MAX(f.salario) as maior_salario_equipe
                FROM iceberg.exercicios.departamentos d
                LEFT JOIN iceberg.exercicios.funcionarios f ON d.nome = f.departamento
                GROUP BY d.nome, d.gerente
            )
            SELECT 
                departamento,
                gerente,
                subordinados_diretos,
                ROUND(salario_medio_equipe, 2) as salario_medio_equipe,
                maior_salario_equipe,
                CASE 
                    WHEN subordinados_diretos = 0 THEN 'Sem equipe'
                    WHEN subordinados_diretos < 3 THEN 'Equipe pequena'
                    WHEN subordinados_diretos < 6 THEN 'Equipe média'
                    ELSE 'Equipe grande'
                END as tamanho_equipe
            FROM org_hierarchy
            ORDER BY subordinados_diretos DESC
        """)
        
        print("🏢 Estrutura organizacional:")
        org_structure.show()
        
        print("\n7️⃣ Listando todas as views temporárias criadas:")
        try:
            views = spark.sql("SHOW TABLES").filter(F.col("isTemporary") == True)
            print("📋 Views temporárias ativas:")
            views.show()
        except Exception as e:
            print(f"⚠️ Não foi possível listar views: {e}")
        
        print("✅ Exercício 17 concluído com sucesso!")
        print("👁️ Demonstramos views temporárias e consultas SQL complexas")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 17: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)