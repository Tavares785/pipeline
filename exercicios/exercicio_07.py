#!/usr/bin/env python3
"""
Exercício 7: Consultar dados da tabela Iceberg
- Fazer consultas SQL na tabela funcionarios
- Usar filtros e agregações
- Demonstrar diferentes tipos de consulta
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
    Função principal do exercício 7
    """
    spark = None
    try:
        print("🔍 Exercício 7: Consultar dados da tabela Iceberg")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_07_Consultar_Iceberg")
        
        # Verificar se a tabela existe
        try:
            spark.sql("DESCRIBE TABLE iceberg.exercicios.funcionarios").show()
            print("✅ Tabela funcionarios encontrada")
        except Exception as e:
            print(f"⚠️ Tabela funcionarios não encontrada: {e}")
            print("💡 Execute primeiro os exercícios 4, 5 e 6 para criar a tabela")
            return False
        
        print("\n1️⃣ Consulta simples - todos os dados:")
        result1 = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios")
        result1.show()
        print(f"📊 Total de registros: {result1.count()}")
        
        print("\n2️⃣ Consulta com filtro - funcionários de TI:")
        result2 = spark.sql("""
            SELECT nome, cargo, salario 
            FROM iceberg.exercicios.funcionarios 
            WHERE departamento = 'TI'
            ORDER BY salario DESC
        """)
        result2.show()
        
        print("\n3️⃣ Consulta com agregação - salário médio por departamento:")
        result3 = spark.sql("""
            SELECT 
                departamento,
                COUNT(*) as total_funcionarios,
                AVG(salario) as salario_medio,
                MIN(salario) as salario_minimo,
                MAX(salario) as salario_maximo
            FROM iceberg.exercicios.funcionarios 
            GROUP BY departamento
            ORDER BY salario_medio DESC
        """)
        result3.show()
        
        print("\n4️⃣ Consulta com condições múltiplas:")
        result4 = spark.sql("""
            SELECT nome, departamento, cargo, salario
            FROM iceberg.exercicios.funcionarios 
            WHERE salario > 6000 
            AND (departamento = 'TI' OR departamento = 'Vendas')
            ORDER BY salario DESC
        """)
        result4.show()
        
        print("\n5️⃣ Usando DataFrame API:")
        df = spark.table("iceberg.exercicios.funcionarios")
        
        # Funcionários com salário acima da média
        salario_medio = df.agg(F.avg("salario")).collect()[0][0]
        print(f"💰 Salário médio da empresa: R$ {salario_medio:.2f}")
        
        acima_da_media = df.filter(F.col("salario") > salario_medio)\
                          .select("nome", "departamento", "salario")\
                          .orderBy(F.desc("salario"))
        
        print(f"\n👑 Funcionários com salário acima da média:")
        acima_da_media.show()
        
        print("\n6️⃣ Estatísticas descritivas:")
        df.describe("salario").show()
        
        print("✅ Exercício 7 concluído com sucesso!")
        print("🔍 Demonstramos consultas SQL e DataFrame API na tabela Iceberg")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 7: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)