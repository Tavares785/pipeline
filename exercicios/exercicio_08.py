#!/usr/bin/env python3
"""
Exercício 8: Atualizar dados específicos na tabela Iceberg
- Usar UPDATE SQL para modificar registros
- Demonstrar atualizações condicionais
- Verificar versionamento Iceberg
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
    Função principal do exercício 8
    """
    spark = None
    try:
        print("🔄 Exercício 8: Atualizar dados na tabela Iceberg")
        print("-" * 50)
        
        # Criar sessão Spark
        spark = create_spark_session("Exercicio_08_Update_Iceberg")
        
        # Verificar se a tabela existe
        try:
            spark.sql("SELECT COUNT(*) FROM iceberg.exercicios.funcionarios").show()
            print("✅ Tabela funcionarios encontrada")
        except Exception as e:
            print(f"⚠️ Tabela funcionarios não encontrada: {e}")
            print("💡 Execute primeiro os exercícios 4, 5 e 6 para criar a tabela")
            return False
        
        print("\n📊 Estado inicial da tabela:")
        spark.sql("SELECT * FROM iceberg.exercicios.funcionarios ORDER BY id").show()
        
        print("\n1️⃣ Atualização simples - aumento de salário para TI:")
        # Atualizar salários do departamento de TI (aumento de 10%)
        spark.sql("""
            UPDATE iceberg.exercicios.funcionarios 
            SET salario = salario * 1.10 
            WHERE departamento = 'TI'
        """)
        
        print("✅ Salários de TI atualizados (+10%)")
        
        print("\n📊 Verificando atualizações - departamento TI:")
        spark.sql("""
            SELECT nome, departamento, salario 
            FROM iceberg.exercicios.funcionarios 
            WHERE departamento = 'TI'
            ORDER BY salario DESC
        """).show()
        
        print("\n2️⃣ Atualização condicional - promoção específica:")
        # Promover Ana para Gerente de Vendas
        spark.sql("""
            UPDATE iceberg.exercicios.funcionarios 
            SET cargo = 'Gerente de Vendas', salario = 8500
            WHERE nome = 'Ana'
        """)
        
        print("✅ Ana promovida para Gerente de Vendas")
        
        print("\n3️⃣ Atualização múltipla - ajuste por cargo:")
        # Aumentar salário de todos os analistas
        spark.sql("""
            UPDATE iceberg.exercicios.funcionarios 
            SET salario = salario + 500
            WHERE cargo LIKE '%Analista%'
        """)
        
        print("✅ Salários de analistas ajustados (+R$ 500)")
        
        print("\n📊 Estado final da tabela após atualizações:")
        result = spark.sql("""
            SELECT nome, departamento, cargo, salario 
            FROM iceberg.exercicios.funcionarios 
            ORDER BY salario DESC
        """)
        result.show()
        
        print("\n4️⃣ Estatísticas após atualizações:")
        stats = spark.sql("""
            SELECT 
                departamento,
                COUNT(*) as funcionarios,
                AVG(salario) as salario_medio,
                MAX(salario) as salario_maximo
            FROM iceberg.exercicios.funcionarios 
            GROUP BY departamento
            ORDER BY salario_medio DESC
        """)
        stats.show()
        
        print("\n5️⃣ Verificando histórico de snapshots:")
        try:
            snapshots = spark.sql("SELECT * FROM iceberg.exercicios.funcionarios.snapshots")
            print(f"📷 Total de snapshots: {snapshots.count()}")
            snapshots.select("snapshot_id", "operation", "summary").show(truncate=False)
        except Exception as e:
            print(f"⚠️ Não foi possível acessar snapshots: {e}")
        
        print("✅ Exercício 8 concluído com sucesso!")
        print("🔄 Demonstramos atualizações SQL na tabela Iceberg")
        return True
        
    except Exception as e:
        print(f"❌ Erro no exercício 8: {e}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)