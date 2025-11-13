#!/usr/bin/env python3
"""
Exercício 12: Inserir dados na tabela particionada
- Inserir dados na tabela funcionarios_por_depto
- Demonstrar inserção em diferentes partições
- Verificar distribuição dos dados
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
    Função principal do exercício 12
    """
    spark = None
    try:
        print("📥 Exercício 12: Inserir dados na tabela particionada")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_12_Insert_Partitioned")
        
        # Verificar se a tabela particionada existe
        try:
            spark.sql("DESCRIBE TABLE iceberg.exercicios.funcionarios_por_depto").show()
            print("✅ Tabela funcionarios_por_depto encontrada")
        except Exception as e:
            print(f"⚠️ Tabela funcionarios_por_depto não encontrada: {e}")
            print("💡 Execute primeiro o exercício 11 para criar a tabela particionada")
            return False
        
        print("\n📊 Estado inicial da tabela particionada:")
        initial_count = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios_por_depto").collect()[0][0]
        print(f"📈 Registros iniciais: {initial_count}")
        
        spark.sql("""
            SELECT departamento, COUNT(*) as funcionarios
            FROM iceberg.exercicios.funcionarios_por_depto
            GROUP BY departamento
            ORDER BY funcionarios DESC
        """).show()
        
        print("\n1️⃣ Inserindo dados do departamento de TI:")
        # Inserir vários funcionários de TI
        ti_data = """
            INSERT INTO iceberg.exercicios.funcionarios_por_depto VALUES
            (101, 'Carlos Pereira', 'Desenvolvedor Python', 'TI', 7200.0),
            (102, 'Ana Costa', 'DevOps Engineer', 'TI', 8200.0),
            (103, 'Bruno Silva', 'Arquiteto de Software', 'TI', 9500.0),
            (104, 'Fernanda Lima', 'QA Tester', 'TI', 5800.0),
            (105, 'Ricardo Santos', 'Tech Lead', 'TI', 10200.0)
        """
        
        spark.sql(ti_data)
        print("✅ Funcionários de TI inseridos")
        
        print("\n2️⃣ Inserindo dados do departamento de Vendas:")
        # Inserir funcionários de Vendas
        vendas_data = """
            INSERT INTO iceberg.exercicios.funcionarios_por_depto VALUES
            (201, 'Juliana Ferreira', 'Representante de Vendas', 'Vendas', 4800.0),
            (202, 'Marcos Oliveira', 'Gerente de Contas', 'Vendas', 7800.0),
            (203, 'Patricia Rocha', 'Consultora de Vendas', 'Vendas', 5200.0),
            (204, 'Eduardo Nunes', 'Diretor Comercial', 'Vendas', 12000.0)
        """
        
        spark.sql(vendas_data)
        print("✅ Funcionários de Vendas inseridos")
        
        print("\n3️⃣ Inserindo dados de novos departamentos:")
        # Inserir funcionários de departamentos novos
        novos_deptos = """
            INSERT INTO iceberg.exercicios.funcionarios_por_depto VALUES
            (301, 'Sofia Almeida', 'Designer UX/UI', 'Design', 6500.0),
            (302, 'Gabriel Torres', 'Motion Designer', 'Design', 5800.0),
            (401, 'Helena Barbosa', 'Analista Financeiro', 'Financeiro', 6200.0),
            (402, 'Andre Machado', 'Controller', 'Financeiro', 8500.0),
            (501, 'Camila Souza', 'Jurídica Senior', 'Jurídico', 7800.0)
        """
        
        spark.sql(novos_deptos)
        print("✅ Funcionários de novos departamentos inseridos")
        
        print("\n4️⃣ Inserindo dados usando DataFrame:")
        # Criar DataFrame e inserir
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("nome", StringType(), True),
            StructField("cargo", StringType(), True),
            StructField("departamento", StringType(), True),
            StructField("salario", DoubleType(), True)
        ])
        
        # Dados do departamento de Marketing
        marketing_data = [
            (601, "Laura Mendes", "Analista de Marketing", "Marketing", 5500.0),
            (602, "Diego Campos", "Coordenador de Marketing", "Marketing", 7000.0),
            (603, "Isabela Ramos", "Social Media", "Marketing", 4200.0)
        ]
        
        df_marketing = spark.createDataFrame(marketing_data, schema)
        
        # Inserir usando DataFrame
        df_marketing.writeTo("iceberg.exercicios.funcionarios_por_depto").append()
        print("✅ Funcionários de Marketing inseridos via DataFrame")
        
        print("\n📊 Estado final da tabela particionada:")
        final_result = spark.sql("""
            SELECT departamento, COUNT(*) as funcionarios, AVG(salario) as salario_medio
            FROM iceberg.exercicios.funcionarios_por_depto
            GROUP BY departamento
            ORDER BY funcionarios DESC
        """)
        final_result.show()
        
        final_count = spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios_por_depto").collect()[0][0]
        print(f"📈 Total de registros: {final_count}")
        print(f"🆕 Novos registros inseridos: {final_count - initial_count}")
        
        print("\n5️⃣ Verificando distribuição por partição:")
        # Mostrar todos os dados organizados por partição
        all_data = spark.sql("""
            SELECT departamento, nome, cargo, salario
            FROM iceberg.exercicios.funcionarios_por_depto
            ORDER BY departamento, salario DESC
        """)
        all_data.show(50)
        
        print("\n6️⃣ Estatísticas detalhadas por departamento:")
        detailed_stats = spark.sql("""
            SELECT 
                departamento,
                COUNT(*) as total_funcionarios,
                MIN(salario) as salario_minimo,
                AVG(salario) as salario_medio,
                MAX(salario) as salario_maximo,
                SUM(salario) as folha_salarial
            FROM iceberg.exercicios.funcionarios_por_depto
            GROUP BY departamento
            ORDER BY folha_salarial DESC
        """)
        detailed_stats.show()
        
        print("\n7️⃣ Top 5 maiores salários:")
        top_salarios = spark.sql("""
            SELECT nome, departamento, cargo, salario
            FROM iceberg.exercicios.funcionarios_por_depto
            ORDER BY salario DESC
            LIMIT 5
        """)
        top_salarios.show()
        
        print("✅ Exercício 12 concluído com sucesso!")
        print("📥 Demonstramos inserção de dados em tabela particionada por departamento")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 12: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)