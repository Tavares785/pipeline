#!/usr/bin/env python3
"""
Exercício 16: Fazer join entre tabelas Iceberg
- Criar uma segunda tabela (departamentos)
- Fazer diferentes tipos de join
- Demonstrar performance de joins em Iceberg
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
    Função principal do exercício 16
    """
    spark = None
    try:
        print("🔗 Exercício 16: Join entre tabelas Iceberg")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_16_Join_Tables")
        
        # Verificar se a tabela funcionarios existe
        try:
            spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios").show()
            print("✅ Tabela funcionarios encontrada")
        except Exception as e:
            print(f"⚠️ Tabela funcionarios não encontrada: {e}")
            print("💡 Execute primeiro os exercícios 4, 5 e 6 para criar a tabela")
            return False
        
        print("\n1️⃣ Criando tabela de departamentos:")
        
        # Criar tabela de departamentos
        create_dept_table = """
        CREATE TABLE IF NOT EXISTS iceberg.exercicios.departamentos (
            id INT,
            nome STRING,
            gerente STRING,
            orcamento DOUBLE,
            localizacao STRING
        ) USING iceberg
        """
        
        spark.sql(create_dept_table)
        print("✅ Tabela departamentos criada")
        
        # Verificar se já tem dados
        dept_count = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.departamentos").collect()[0][0]
        
        if dept_count == 0:
            print("\n2️⃣ Inserindo dados na tabela departamentos:")
            
            dept_data = """
            INSERT INTO iceberg.exercicios.departamentos VALUES
            (1, 'TI', 'Ricardo Santos', 150000.0, 'São Paulo - SP'),
            (2, 'Vendas', 'Eduardo Nunes', 200000.0, 'Rio de Janeiro - RJ'),
            (3, 'RH', 'Lucia Ferreira', 80000.0, 'São Paulo - SP'),
            (4, 'Marketing', 'Diego Campos', 120000.0, 'São Paulo - SP'),
            (5, 'Financeiro', 'Andre Machado', 100000.0, 'São Paulo - SP'),
            (6, 'Design', 'Sofia Almeida', 90000.0, 'São Paulo - SP'),
            (7, 'Jurídico', 'Camila Souza', 70000.0, 'Brasília - DF')
            """
            
            spark.sql(dept_data)
            print("✅ Dados inseridos na tabela departamentos")
        else:
            print(f"ℹ️ Tabela departamentos já possui {dept_count} registros")
        
        print("\n3️⃣ Visualizando tabela departamentos:")
        spark.sql("""
            SELECT id, nome, gerente, orcamento, localizacao
            FROM iceberg.exercicios.departamentos
            ORDER BY orcamento DESC
        """).show()
        
        print("\n4️⃣ INNER JOIN - funcionários com departamentos:")
        inner_join = spark.sql("""
            SELECT 
                f.nome as funcionario,
                f.cargo,
                f.salario,
                d.nome as departamento,
                d.gerente,
                d.orcamento as orcamento_dept,
                d.localizacao
            FROM iceberg.exercicios.funcionarios f
            INNER JOIN iceberg.exercicios.departamentos d 
                ON f.departamento = d.nome
            ORDER BY f.salario DESC
        """)
        
        print("🔗 INNER JOIN resultado:")
        inner_join.show()
        
        print("\n5️⃣ LEFT JOIN - todos funcionários, com ou sem departamento:")
        left_join = spark.sql("""
            SELECT 
                f.nome as funcionario,
                f.departamento as depto_funcionario,
                f.cargo,
                f.salario,
                d.nome as depto_tabela,
                d.gerente,
                CASE 
                    WHEN d.nome IS NULL THEN 'Departamento não encontrado'
                    ELSE 'Departamento válido'
                END as status_depto
            FROM iceberg.exercicios.funcionarios f
            LEFT JOIN iceberg.exercicios.departamentos d 
                ON f.departamento = d.nome
            ORDER BY f.departamento, f.salario DESC
        """)
        
        print("🔗 LEFT JOIN resultado:")
        left_join.show()
        
        print("\n6️⃣ RIGHT JOIN - todos departamentos, com ou sem funcionários:")
        right_join = spark.sql("""
            SELECT 
                d.nome as departamento,
                d.gerente,
                d.orcamento,
                d.localizacao,
                COUNT(f.id) as total_funcionarios,
                COALESCE(SUM(f.salario), 0) as folha_salarial
            FROM iceberg.exercicios.funcionarios f
            RIGHT JOIN iceberg.exercicios.departamentos d 
                ON f.departamento = d.nome
            GROUP BY d.nome, d.gerente, d.orcamento, d.localizacao
            ORDER BY total_funcionarios DESC
        """)
        
        print("🔗 RIGHT JOIN com agregação:")
        right_join.show()
        
        print("\n7️⃣ Análise de orçamento vs folha salarial:")
        budget_analysis = spark.sql("""
            SELECT 
                d.nome as departamento,
                d.orcamento,
                COALESCE(SUM(f.salario), 0) as folha_salarial,
                d.orcamento - COALESCE(SUM(f.salario), 0) as saldo_orcamento,
                CASE 
                    WHEN COALESCE(SUM(f.salario), 0) > d.orcamento THEN 'Acima do orçamento'
                    WHEN COALESCE(SUM(f.salario), 0) = 0 THEN 'Sem funcionários'
                    ELSE 'Dentro do orçamento'
                END as status_orcamento,
                ROUND((COALESCE(SUM(f.salario), 0) / d.orcamento) * 100, 2) as percentual_usado
            FROM iceberg.exercicios.departamentos d
            LEFT JOIN iceberg.exercicios.funcionarios f 
                ON d.nome = f.departamento
            GROUP BY d.nome, d.orcamento
            ORDER BY percentual_usado DESC
        """)
        
        print("💰 Análise orçamentária:")
        budget_analysis.show()
        
        print("\n8️⃣ Join complexo com window functions:")
        complex_join = spark.sql("""
            SELECT 
                f.nome as funcionario,
                f.cargo,
                f.salario,
                d.nome as departamento,
                d.gerente,
                AVG(f.salario) OVER (PARTITION BY d.nome) as salario_medio_dept,
                ROW_NUMBER() OVER (PARTITION BY d.nome ORDER BY f.salario DESC) as ranking_dept,
                DENSE_RANK() OVER (ORDER BY f.salario DESC) as ranking_geral
            FROM iceberg.exercicios.funcionarios f
            INNER JOIN iceberg.exercicios.departamentos d 
                ON f.departamento = d.nome
        """)
        
        print("🏆 Join com ranking:")
        complex_join.show()
        
        print("\n9️⃣ Self join - comparando funcionários do mesmo departamento:")
        self_join = spark.sql("""
            SELECT DISTINCT
                f1.nome as funcionario1,
                f1.salario as salario1,
                f2.nome as funcionario2,
                f2.salario as salario2,
                f1.departamento,
                ABS(f1.salario - f2.salario) as diferenca_salarial
            FROM iceberg.exercicios.funcionarios f1
            JOIN iceberg.exercicios.funcionarios f2 
                ON f1.departamento = f2.departamento 
                AND f1.id < f2.id
            WHERE ABS(f1.salario - f2.salario) > 1000
            ORDER BY f1.departamento, diferenca_salarial DESC
        """)
        
        print("👥 Comparação salarial entre colegas (diferença > R$ 1000):")
        self_join.show()
        
        print("\n🔟 Performance de join com explain:")
        print("📋 Plano de execução do join:")
        try:
            join_query = spark.sql("""
                SELECT f.nome, d.nome as departamento, f.salario, d.orcamento
                FROM iceberg.exercicios.funcionarios f
                JOIN iceberg.exercicios.departamentos d ON f.departamento = d.nome
            """)
            join_query.explain()
        except Exception as e:
            print(f"⚠️ Não foi possível mostrar o plano: {e}")
        
        print("✅ Exercício 16 concluído com sucesso!")
        print("🔗 Demonstramos diferentes tipos de join entre tabelas Iceberg")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 16: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)