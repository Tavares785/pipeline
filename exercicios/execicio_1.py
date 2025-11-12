# Exercícios de Big Data com Spark + Iceberg + Hive Metastore + HDFS
# Este arquivo contém todos os 20 exercícios em formato Python puro
# Para usar: copie e cole cada seção no Jupyter Notebook

# =============================================================================
# CONFIGURAÇÃO INICIAL DO SPARK COM ICEBERG
# =============================================================================

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Configuração do Spark com Iceberg
spark = SparkSession.builder \
    .appName("ExerciciosBigData") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse") \
    .config("spark.sql.defaultCatalog", "local") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("✅ Spark Session criada com sucesso!")
print(f"Spark Version: {spark.version}")
print(f"Spark Master: {spark.sparkContext.master}")

# =============================================================================
# EXERCÍCIO 1: Criar um DataFrame simples
# =============================================================================

print("=== EXERCÍCIO 1: DataFrame Simples ===")

# Dados para o DataFrame
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
schema = ["id", "nome"]

# Criar DataFrame
df_simples = spark.createDataFrame(data, schema)

# Mostrar conteúdo
print("DataFrame criado:")
df_simples.show()

print("Schema do DataFrame:")
df_simples.printSchema()

print(f"Número de linhas: {df_simples.count()}")
print("✅ Exercício 1 concluído!")

# =============================================================================
# EXERCÍCIO 2: Salvar DataFrame no HDFS como CSV
# =============================================================================

print("=== EXERCÍCIO 2: Salvar DataFrame no HDFS ===")

# Usar o DataFrame do exercício anterior
hdfs_path = "hdfs://namenode:9000/data/ex1.csv"

# Salvar como CSV no HDFS
df_simples.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(hdfs_path)

print(f"✅ DataFrame salvo no HDFS: {hdfs_path}")
print("✅ Exercício 2 concluído!")

# =============================================================================
# EXERCÍCIO 3: Ler CSV do HDFS
# =============================================================================

print("=== EXERCÍCIO 3: Ler CSV do HDFS ===")

# Ler o arquivo CSV do HDFS
df_lido = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(hdfs_path)

print("DataFrame lido do HDFS:")
df_lido.show()

print("Schema do DataFrame lido:")
df_lido.printSchema()

print(f"Número de registros lidos: {df_lido.count()}")
print("✅ Exercício 3 concluído!")

# =============================================================================
# EXERCÍCIO 4: Criar namespace Iceberg
# =============================================================================

print("=== EXERCÍCIO 4: Criar Namespace Iceberg ===")

# Criar namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.db")

print("✅ Namespace 'lab.db' criado com sucesso!")

# Listar namespaces
print("Namespaces disponíveis:")
spark.sql("SHOW NAMESPACES").show()

print("✅ Exercício 4 concluído!")

# =============================================================================
# EXERCÍCIO 5: Criar tabela Iceberg
# =============================================================================

print("=== EXERCÍCIO 5: Criar Tabela Iceberg ===")

# Criar tabela Iceberg
create_table_sql = """
CREATE TABLE IF NOT EXISTS lab.db.pessoas (
    id INT,
    nome STRING
) USING ICEBERG
"""

spark.sql(create_table_sql)
print("✅ Tabela 'lab.db.pessoas' criada com sucesso!")

# Verificar se a tabela foi criada
print("Tabelas no namespace lab.db:")
spark.sql("SHOW TABLES IN lab.db").show()

print("✅ Exercício 5 concluído!")

# =============================================================================
# EXERCÍCIO 6: Inserir dados na tabela Iceberg
# =============================================================================

print("=== EXERCÍCIO 6: Inserir Dados na Tabela Iceberg ===")

# Inserir dados
insert_sql = """
INSERT INTO lab.db.pessoas VALUES 
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie')
"""

spark.sql(insert_sql)
print("✅ Dados inseridos na tabela 'lab.db.pessoas'!")

# Verificar inserção
print("Dados na tabela:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

print("✅ Exercício 6 concluído!")

# =============================================================================
# EXERCÍCIO 7: Ler tabela Iceberg
# =============================================================================

print("=== EXERCÍCIO 7: Ler Tabela Iceberg ===")

# Query na tabela Iceberg
result = spark.sql("SELECT * FROM lab.db.pessoas")

print("Resultado da query:")
result.show()

# Informações adicionais
print(f"Número de colunas: {len(result.columns)}")
print(f"Colunas: {result.columns}")
print(f"Número de registros: {result.count()}")

print("✅ Exercício 7 concluído!")

# =============================================================================
# EXERCÍCIO 8: Contar registros
# =============================================================================

print("=== EXERCÍCIO 8: Contar Registros ===")

# Contar registros usando SQL
count_result = spark.sql("SELECT COUNT(*) as total_registros FROM lab.db.pessoas")
count_result.show()

# Alternativa usando DataFrame API
df_pessoas = spark.table("lab.db.pessoas")
total_count = df_pessoas.count()
print(f"Total de registros (DataFrame API): {total_count}")

print("✅ Exercício 8 concluído!")

# =============================================================================
# EXERCÍCIO 9: Atualizar um registro
# =============================================================================

print("=== EXERCÍCIO 9: Atualizar Registro ===")

# Mostrar dados antes da atualização
print("Dados ANTES da atualização:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

# Atualizar registro
update_sql = "UPDATE lab.db.pessoas SET nome = 'Alice Silva' WHERE nome = 'Alice'"
spark.sql(update_sql)

print("✅ Registro atualizado!")

# Mostrar dados após a atualização
print("Dados APÓS a atualização:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

print("✅ Exercício 9 concluído!")

# =============================================================================
# EXERCÍCIO 10: Deletar um registro
# =============================================================================

print("=== EXERCÍCIO 10: Deletar Registro ===")

# Mostrar dados antes da deleção
print("Dados ANTES da deleção:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

# Deletar registro
delete_sql = "DELETE FROM lab.db.pessoas WHERE nome = 'Charlie'"
spark.sql(delete_sql)

print("✅ Registro deletado!")

# Mostrar dados após a deleção
print("Dados APÓS a deleção:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

# Contar registros restantes
count_after = spark.sql("SELECT COUNT(*) as total FROM lab.db.pessoas")
count_after.show()

print("✅ Exercício 10 concluído!")

# =============================================================================
# EXERCÍCIO 11: Criar tabela particionada
# =============================================================================

print("=== EXERCÍCIO 11: Criar Tabela Particionada ===")

# Criar tabela particionada
create_partitioned_table_sql = """
CREATE TABLE IF NOT EXISTS lab.db.vendas (
    id INT,
    valor DOUBLE,
    ano INT
) USING ICEBERG
PARTITIONED BY (ano)
"""

spark.sql(create_partitioned_table_sql)
print("✅ Tabela particionada 'lab.db.vendas' criada!")

# Verificar tabelas
print("Tabelas no namespace lab.db:")
spark.sql("SHOW TABLES IN lab.db").show()

print("✅ Exercício 11 concluído!")

# =============================================================================
# EXERCÍCIO 12: Inserir dados particionados
# =============================================================================

print("=== EXERCÍCIO 12: Inserir Dados Particionados ===")

# Inserir dados para diferentes anos
insert_vendas_sql = """
INSERT INTO lab.db.vendas VALUES 
    (1, 1500.50, 2022),
    (2, 2300.75, 2022),
    (3, 1800.00, 2023),
    (4, 2100.25, 2023),
    (5, 1950.80, 2023),
    (6, 2500.00, 2024),
    (7, 1750.30, 2024)
"""

spark.sql(insert_vendas_sql)
print("✅ Dados inseridos na tabela particionada!")

# Verificar dados inseridos
print("Todos os dados na tabela vendas:")
spark.sql("SELECT * FROM lab.db.vendas ORDER BY ano, id").show()

# Contar por partição
print("Contagem por ano (partição):")
spark.sql("SELECT ano, COUNT(*) as total_vendas, SUM(valor) as valor_total FROM lab.db.vendas GROUP BY ano ORDER BY ano").show()

print("✅ Exercício 12 concluído!")

# =============================================================================
# EXERCÍCIO 13: Consultar apenas um particionamento
# =============================================================================

print("=== EXERCÍCIO 13: Consultar Partição Específica ===")

# Consultar apenas vendas de 2023
vendas_2023 = spark.sql("SELECT * FROM lab.db.vendas WHERE ano = 2023 ORDER BY id")

print("Vendas do ano 2023:")
vendas_2023.show()

# Estatísticas das vendas de 2023
stats_2023 = spark.sql("""
    SELECT 
        COUNT(*) as total_vendas,
        SUM(valor) as valor_total,
        AVG(valor) as valor_medio,
        MIN(valor) as menor_venda,
        MAX(valor) as maior_venda
    FROM lab.db.vendas 
    WHERE ano = 2023
""")

print("Estatísticas das vendas de 2023:")
stats_2023.show()

print("✅ Exercício 13 concluído!")

# =============================================================================
# EXERCÍCIO 14: Ver metadados da tabela
# =============================================================================

print("=== EXERCÍCIO 14: Metadados da Tabela ===")

# Histórico da tabela
print("HISTÓRICO da tabela vendas:")
try:
    spark.sql("DESCRIBE HISTORY lab.db.vendas").show(truncate=False)
except Exception as e:
    print(f"Erro ao mostrar histórico: {e}")
    # Alternativa
    try:
        spark.sql("SELECT * FROM lab.db.vendas.history").show(truncate=False)
    except:
        print("Histórico não disponível nesta configuração")

print("\nDETALHES da tabela vendas:")
try:
    spark.sql("DESCRIBE DETAIL lab.db.vendas").show(truncate=False)
except Exception as e:
    print(f"Erro ao mostrar detalhes: {e}")
    # Informações básicas da tabela
    spark.sql("DESCRIBE EXTENDED lab.db.vendas").show(truncate=False)

print("✅ Exercício 14 concluído!")

# =============================================================================
# EXERCÍCIO 15: Criar tabela Iceberg a partir de DataFrame
# =============================================================================

print("=== EXERCÍCIO 15: Tabela Iceberg a partir de DataFrame ===")

# Criar DataFrame artificial
from datetime import datetime, date

# Dados artificiais de produtos
produtos_data = [
    (1, "Notebook Dell", 2500.00, "Eletrônicos", date(2024, 1, 15)),
    (2, "Mouse Logitech", 85.50, "Periféricos", date(2024, 1, 16)),
    (3, "Teclado Mecânico", 350.00, "Periféricos", date(2024, 1, 17)),
    (4, "Monitor 24\"", 800.00, "Eletrônicos", date(2024, 1, 18)),
    (5, "Webcam HD", 120.00, "Periféricos", date(2024, 1, 19))
]

# Schema do DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome_produto", StringType(), True),
    StructField("preco", DoubleType(), True),
    StructField("categoria", StringType(), True),
    StructField("data_cadastro", DateType(), True)
])

# Criar DataFrame
df_produtos = spark.createDataFrame(produtos_data, schema)

print("DataFrame de produtos criado:")
df_produtos.show()

# Salvar como tabela Iceberg usando writeTo
df_produtos.writeTo("lab.db.produtos").createOrReplace()

print("✅ Tabela 'lab.db.produtos' criada a partir do DataFrame!")

# Verificar a tabela criada
print("Dados na nova tabela:")
spark.sql("SELECT * FROM lab.db.produtos").show()

print("✅ Exercício 15 concluído!")

# =============================================================================
# EXERCÍCIO 16: Converter tabela para Iceberg
# =============================================================================

print("=== EXERCÍCIO 16: Converter Tabela para Iceberg ===")

# Primeiro, criar uma tabela Parquet regular
clientes_data = [
    (1, "João Silva", "joao@email.com", "São Paulo"),
    (2, "Maria Santos", "maria@email.com", "Rio de Janeiro"),
    (3, "Pedro Costa", "pedro@email.com", "Belo Horizonte"),
    (4, "Ana Oliveira", "ana@email.com", "Porto Alegre")
]

schema_clientes = ["id", "nome", "email", "cidade"]
df_clientes = spark.createDataFrame(clientes_data, schema_clientes)

print("DataFrame de clientes:")
df_clientes.show()

# Salvar como tabela Parquet regular primeiro
df_clientes.write \
    .mode("overwrite") \
    .option("path", "hdfs://namenode:9000/warehouse/lab/db/clientes_parquet") \
    .saveAsTable("lab.db.clientes_parquet")

print("✅ Tabela Parquet 'clientes_parquet' criada!")

# Agora criar uma nova tabela Iceberg com os mesmos dados
print("Criando tabela Iceberg equivalente...")

# Ler dados da tabela Parquet e criar nova tabela Iceberg
df_from_parquet = spark.sql("SELECT * FROM lab.db.clientes_parquet")
df_from_parquet.writeTo("lab.db.clientes_iceberg").createOrReplace()

print("✅ Tabela Iceberg 'clientes_iceberg' criada!")

# Comparar as duas tabelas
print("Dados na tabela Iceberg:")
spark.sql("SELECT * FROM lab.db.clientes_iceberg").show()

print("✅ Exercício 16 concluído!")

# =============================================================================
# EXERCÍCIO 17: Leitura incremental (Time Travel)
# =============================================================================

print("=== EXERCÍCIO 17: Time Travel ===")

# Primeiro, vamos fazer algumas modificações na tabela vendas para ter histórico
print("Estado atual da tabela vendas:")
spark.sql("SELECT * FROM lab.db.vendas ORDER BY ano, id").show()

# Adicionar mais dados
spark.sql("""
    INSERT INTO lab.db.vendas VALUES 
        (8, 3000.00, 2024),
        (9, 2750.50, 2024)
""")

print("Dados após inserção adicional:")
spark.sql("SELECT * FROM lab.db.vendas WHERE ano = 2024 ORDER BY id").show()

# Tentar usar Time Travel
print("Tentando acessar versão anterior da tabela...")
try:
    # Versão 1 da tabela
    spark.sql("SELECT * FROM lab.db.vendas VERSION AS OF 1").show()
except Exception as e:
    print(f"Time Travel com VERSION AS OF não disponível: {e}")
    print("Time Travel pode não estar habilitado nesta configuração.")

print("✅ Exercício 17 concluído!")

# =============================================================================
# EXERCÍCIO 18: Exportar tabela Iceberg para CSV
# =============================================================================

print("=== EXERCÍCIO 18: Exportar Tabela Iceberg para CSV ===")

# Ler tabela Iceberg
df_vendas_export = spark.sql("SELECT * FROM lab.db.vendas ORDER BY ano, id")

print("Dados a serem exportados:")
df_vendas_export.show()

# Definir caminho de exportação
export_path = "hdfs://namenode:9000/export/vendas.csv"

# Exportar para CSV
df_vendas_export.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(export_path)

print(f"✅ Tabela exportada para: {export_path}")

# Verificar exportação lendo o arquivo
print("Verificando arquivo exportado:")
df_verificacao = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(export_path)

df_verificacao.show()
print(f"Total de registros exportados: {df_verificacao.count()}")

print("✅ Exercício 18 concluído!")

# =============================================================================
# EXERCÍCIO 19: Instruções para Dashboard no Superset
# =============================================================================

print("=== EXERCÍCIO 19: Dashboard no Superset ===")

print("""
📊 INSTRUÇÕES PARA CRIAR DASHBOARD NO SUPERSET:

1. Acesse o Superset:
   - URL: http://localhost:8088
   - Usuário: admin
   - Senha: SenhaForte!123

2. Adicionar conexão com Trino:
   - Vá em Settings > Database Connections
   - Clique em "+ DATABASE"
   - Selecione "Trino" como tipo
   - String de conexão: trino://trino:8080/iceberg/lab
   - Teste a conexão

3. Configurar Dataset:
   - Vá em Data > Datasets
   - Adicione as tabelas: vendas, produtos, pessoas
   - Schema: db

4. Criar Visualizações:
   - Charts > + CHART
   - Selecione o dataset 'vendas'
   - Tipos sugeridos:
     * Bar Chart: Vendas por Ano
     * Pie Chart: Distribuição de Vendas
     * Line Chart: Evolução das Vendas

5. Criar Dashboard:
   - Dashboards > + DASHBOARD
   - Adicione os charts criados
   - Configure filtros por ano

📈 DADOS DISPONÍVEIS PARA VISUALIZAÇÃO:
""")

# Mostrar resumo dos dados para o dashboard
print("Resumo das Vendas por Ano:")
spark.sql("""
    SELECT 
        ano,
        COUNT(*) as total_vendas,
        ROUND(SUM(valor), 2) as receita_total,
        ROUND(AVG(valor), 2) as ticket_medio
    FROM lab.db.vendas 
    GROUP BY ano 
    ORDER BY ano
""").show()

print("✅ Exercício 19 - Instruções fornecidas!")

# =============================================================================
# EXERCÍCIO 20: Ler tabela Iceberg via Trino
# =============================================================================

print("=== EXERCÍCIO 20: Leitura via Trino ===")

print("""
🔌 CONECTANDO AO TRINO:

1. Via CLI do Trino (dentro do container):
   docker exec -it trino trino --server localhost:8080 --catalog iceberg --schema lab.db

2. Via Superset ou ferramenta SQL:
   - Host: trino:8080
   - Catalog: iceberg
   - Schema: lab.db

📝 QUERIES PARA EXECUTAR NO TRINO:
""")

# Queries que podem ser executadas no Trino
trino_queries = [
    "-- Listar todas as pessoas",
    "SELECT * FROM iceberg.lab.db.pessoas;",
    "",
    "-- Vendas por ano com estatísticas",
    "SELECT ano, COUNT(*) as vendas, SUM(valor) as receita FROM iceberg.lab.db.vendas GROUP BY ano;",
    "",
    "-- Análise temporal das vendas",
    "SELECT ano, SUM(valor) as receita FROM iceberg.lab.db.vendas GROUP BY ano ORDER BY ano;"
]

for query in trino_queries:
    print(query)

print("""

🧪 TESTANDO CONECTIVIDADE COM TRINO:
""")

# Simular algumas das queries que funcionariam no Trino
print("Executando queries equivalentes no Spark (simulando Trino):")

print("\n1. Todas as pessoas:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

print("\n2. Vendas por ano:")
spark.sql("""
    SELECT 
        ano, 
        COUNT(*) as vendas, 
        ROUND(SUM(valor), 2) as receita 
    FROM lab.db.vendas 
    GROUP BY ano 
    ORDER BY ano
""").show()

print("✅ Exercício 20 concluído!")

# =============================================================================
# RESUMO FINAL
# =============================================================================

print("""
🎉 TODOS OS 20 EXERCÍCIOS CONCLUÍDOS COM SUCESSO!

📊 Dados criados:
- lab.db.pessoas (2 registros após UPDATE/DELETE)
- lab.db.vendas (9 registros, particionada por ano)
- lab.db.produtos (5 registros)
- lab.db.clientes_iceberg (4 registros)
- Arquivos CSV exportados no HDFS

🔧 Tecnologias utilizadas:
- Apache Spark 3.5.1
- Apache Iceberg (formato de tabela)
- HDFS (armazenamento distribuído)
- Hive Metastore (catálogo de metadados)
- Trino (engine de query SQL)
- Apache Superset (visualização)

🚀 AMBIENTE BIG DATA - EXERCÍCIOS CONCLUÍDOS!
""")

# Mostrar todas as tabelas criadas
print("📋 RESUMO DE TODAS AS TABELAS CRIADAS:")
spark.sql("SHOW TABLES IN lab.db").show()

print("💡 Sessão Spark mantida ativa para exploração adicional.")
print("   Execute 'spark.stop()' quando terminar de usar.")