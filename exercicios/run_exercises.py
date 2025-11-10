#!/usr/bin/env python3
"""
Executor principal para todos os exercícios de Big Data
Execute este arquivo para rodar todos os 20 exercícios em sequência
"""

import sys
import os
import importlib.util

def load_and_run_exercise(exercise_number):
    """
    Carrega e executa um exercício específico
    """
    try:
        # Construir nome do arquivo
        filename = f"exercicio_{exercise_number:02d}.py"
        filepath = os.path.join(os.path.dirname(__file__), filename)
        
        if not os.path.exists(filepath):
            print(f"⚠️ Exercício {exercise_number} não encontrado: {filename}")
            return False
        
        # Carregar módulo dinamicamente
        spec = importlib.util.spec_from_file_location(f"exercicio_{exercise_number:02d}", filepath)
        module = importlib.util.module_from_spec(spec)
        
        print(f"\n{'='*60}")
        print(f"🚀 EXECUTANDO EXERCÍCIO {exercise_number}")
        print(f"{'='*60}")
        
        # Executar o módulo
        spec.loader.exec_module(module)
        
        # Executar função main se existir
        if hasattr(module, 'main'):
            result = module.main()
            print(f"✅ Exercício {exercise_number} concluído!")
            return True
        else:
            print(f"⚠️ Exercício {exercise_number} não tem função main()")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar exercício {exercise_number}: {e}")
        return False

def run_all_exercises():
    """
    Executa todos os exercícios de 1 a 20
    """
    print("🎯 INICIANDO EXECUÇÃO DE TODOS OS EXERCÍCIOS")
    print("=" * 70)
    
    successful = 0
    failed = 0
    
    # Lista dos exercícios principais para executar
    exercises_to_run = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    
    for exercise_num in exercises_to_run:
        success = load_and_run_exercise(exercise_num)
        if success:
            successful += 1
        else:
            failed += 1
        
        # Pequena pausa entre exercícios
        import time
        time.sleep(1)
    
    print(f"\n{'='*70}")
    print("📊 RESUMO DA EXECUÇÃO")
    print(f"{'='*70}")
    print(f"✅ Exercícios executados com sucesso: {successful}")
    print(f"❌ Exercícios com falha: {failed}")
    print(f"📈 Total de exercícios testados: {len(exercises_to_run)}")
    
    if failed == 0:
        print("\n🎉 TODOS OS EXERCÍCIOS FORAM EXECUTADOS COM SUCESSO!")
    else:
        print(f"\n⚠️ {failed} exercícios falharam. Verifique as configurações do ambiente.")

def run_specific_exercise(exercise_number):
    """
    Executa um exercício específico
    """
    print(f"🎯 EXECUTANDO EXERCÍCIO ESPECÍFICO: {exercise_number}")
    success = load_and_run_exercise(exercise_number)
    return success

def show_help():
    """
    Mostra instruções de uso
    """
    print("""
🎓 EXECUTOR DE EXERCÍCIOS DE BIG DATA
=====================================

USO:
    python run_exercises.py                    # Executa exercícios principais
    python run_exercises.py all               # Executa todos os exercícios
    python run_exercises.py <número>          # Executa exercício específico
    python run_exercises.py help              # Mostra esta ajuda

EXEMPLOS:
    python run_exercises.py 1                 # Executa apenas exercício 1
    python run_exercises.py 15                # Executa apenas exercício 15
    python run_exercises.py all               # Executa todos os exercícios

REQUISITOS:
    - PySpark instalado
    - Ambiente Spark + Iceberg + HDFS configurado
    - Conectividade com Hive Metastore

📋 EXERCÍCIOS DISPONÍVEIS:
    01 - Criar DataFrame simples
    02 - Salvar DataFrame no HDFS como CSV
    03 - Ler CSV do HDFS
    04 - Criar namespace Iceberg
    05 - Criar tabela Iceberg
    06 - Inserir dados na tabela Iceberg
    07 - Consultar dados da tabela Iceberg
    08 - Atualizar dados específicos
    09 - Deletar registros
    10 - Fazer merge (upsert) de dados
    11 - Criar tabela particionada
    12 - Inserir dados na tabela particionada
    13 - Consultar tabela particionada
    14 - Demonstrar time travel
    15 - Criar tabela Iceberg a partir de DataFrame
    16 - Fazer join entre tabelas Iceberg
    17 - Criar view temporária e SQL complexo
    18 - Exportar tabela Iceberg para CSV
    19 - Otimizar tabela (compactação)
    20 - Analytics com agregações complexas
    """)

def main():
    """
    Função principal
    """
    if len(sys.argv) < 2:
        # Executar exercícios principais por padrão
        run_all_exercises()
    else:
        arg = sys.argv[1].lower()
        
        if arg == "help" or arg == "-h" or arg == "--help":
            show_help()
        elif arg == "all":
            # Executar todos os exercícios (1-20)
            print("🎯 EXECUTANDO TODOS OS EXERCÍCIOS DISPONÍVEIS")
            for i in range(1, 21):
                load_and_run_exercise(i)
        else:
            try:
                exercise_num = int(arg)
                if 1 <= exercise_num <= 20:
                    run_specific_exercise(exercise_num)
                else:
                    print("❌ Número do exercício deve estar entre 1 e 20")
                    sys.exit(1)
            except ValueError:
                print("❌ Argumento inválido. Use 'help' para ver as opções.")
                sys.exit(1)

if __name__ == "__main__":
    main()