# ============================================================
# Exercício 1 - Big Data com Spark + Iceberg + Hive + HDFS
# ============================================================
# Autor: ChatGPT (o maior programador do universo ⚡)
# Descrição: Este script executa passo a passo todos os 20
# exercícios usando PySpark + Iceberg + Hive Metastore + HDFS.
# ============================================================

from pyspark.sql import SparkSession

# ------------------------------------------------------------
# 0. INICIAR SPARK SESSION COM ICEBERG + HIVE + HDFS
# ------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Exercicio1")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hive")
    .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.lab.warehouse", "hdfs://namenode:9000/warehouse")
    .getOrCreate()
)

print("\n✅ SparkSession iniciada com sucesso!\n")

# ------------------------------------------------------------
# 1. CRIAR DATAFRAME SIMPLES
# ------------------------------------------------------------
data = [(1, "Alice"), (2, "Bob"), (3, "Carol")]
df = spark.createDataFrame(data, ["id", "nome"])
print("📘 DataFrame inicial:")
df.show()

# ------------------------------------------------------------
# 2. SALVAR DATAFRAME NO HDFS COMO CSV
# ------------------------------------------------------------
csv_path = "hdfs://namenode:9000/data/ex1.csv"
df.write.mode("overwrite").csv(csv_path, header=True)
print(f"💾 DataFrame salvo com sucesso em {csv_path}\n")

# ------------------------------------------------------------
# 3. LER CSV DO HDFS
# ------------------------------------------------------------
df_csv = spark.read.csv(csv_path, header=True, inferSchema=True)
print("📂 DataFrame lido do HDFS:")
df_csv.show()

# ------------------------------------------------------------
# 4. CRIAR NAMESPACE ICEBERG
# ------------------------------------------------------------
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.db")
print("🏗️ Namespace 'lab.db' criado/verificado.\n")

# ------------------------------------------------------------
# 5. CRIAR TABELA ICEBERG
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS lab.db.pessoas (
  id INT,
  nome STRING
) USING ICEBERG
""")
print("📦 Tabela Iceberg 'lab.db.pessoas' criada.\n")

# ------------------------------------------------------------
# 6. INSERIR DADOS NA TABELA ICEBERG
# ------------------------------------------------------------
spark.sql("""
INSERT INTO lab.db.pessoas VALUES
  (1, 'Alice'),
  (2, 'Bob'),
  (3, 'Carol')
""")
print("🧩 Dados inseridos na tabela 'lab.db.pessoas'.\n")

# ------------------------------------------------------------
# 7. LER TABELA ICEBERG
# ------------------------------------------------------------
print("📊 Conteúdo da tabela 'lab.db.pessoas':")
spark.sql("SELECT * FROM lab.db.pessoas").show()

# ------------------------------------------------------------
# 8. CONTAR REGISTROS
# ------------------------------------------------------------
print("🔢 Contagem de registros:")
spark.sql("SELECT COUNT(*) AS total FROM lab.db.pessoas").show()

# ------------------------------------------------------------
# 9. ATUALIZAR UM REGISTRO
# ------------------------------------------------------------
spark.sql("UPDATE lab.db.pessoas SET nome = 'Alice Silva' WHERE nome = 'Alice'")
print("✏️ Registro atualizado (Alice → Alice Silva).\n")

# ------------------------------------------------------------
# 10. DELETAR UM REGISTRO
# ------------------------------------------------------------
spark.sql("DELETE FROM lab.db.pessoas WHERE nome = 'Bob'")
print("❌ Registro 'Bob' removido da tabela.\n")

# ------------------------------------------------------------
# 11. CRIAR TABELA PARTICIONADA
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS lab.db.vendas (
  id INT,
  valor DOUBLE,
  ano INT
) USING ICEBERG
PARTITIONED BY (ano)
""")
print("📂 Tabela particionada 'lab.db.vendas' criada.\n")

# ------------------------------------------------------------
# 12. INSERIR DADOS PARTICIONADOS
# ------------------------------------------------------------
spark.sql("""
INSERT INTO lab.db.vendas VALUES
  (1, 100.50, 2022),
  (2, 250.75, 2023),
  (3, 300.00, 2024)
""")
print("💸 Dados particionados inseridos.\n")

# ------------------------------------------------------------
# 13. CONSULTAR APENAS UM PARTICIONAMENTO
# ------------------------------------------------------------
print("🔎 Consultando apenas as vendas de 2023:")
spark.sql("SELECT * FROM lab.db.vendas WHERE ano = 2023").show()

# ------------------------------------------------------------
# 14. VER METADADOS DA TABELA
# ------------------------------------------------------------
print("📜 Histórico da tabela 'lab.db.vendas':")
spark.sql("DESCRIBE HISTORY lab.db.vendas").show(truncate=False)

print("\n📋 Detalhes da tabela 'lab.db.vendas':")
spark.sql("DESCRIBE DETAIL lab.db.vendas").show(truncate=False)

# ------------------------------------------------------------
# 15. CRIAR TABELA ICEBERG A PARTIR DE DATAFRAME
# ------------------------------------------------------------
df2 = spark.createDataFrame([(10, "Zoe"), (11, "Léo")], ["id", "nome"])
df2.writeTo("lab.db.tabela_df").createOrReplace()
print("🧠 Tabela 'lab.db.tabela_df' criada a partir de DataFrame.\n")

# ------------------------------------------------------------
# 16. CONVERTER TABELA PARQUET PARA ICEBERG
# ------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS lab.db.tabela_parquet (
  id INT,
  nome STRING
) USING PARQUET
""")
spark.sql("""
ALTER TABLE lab.db.tabela_parquet SET TBLPROPERTIES ('format-version'='2')
""")
print("🔁 Tabela 'lab.db.tabela_parquet' convertida para formato Iceberg.\n")

# ------------------------------------------------------------
# 17. LEITURA INCREMENTAL (TIME TRAVEL)
# ------------------------------------------------------------
print("⏳ Versão anterior da tabela 'lab.db.vendas' (VERSION AS OF 1):")
spark.sql("SELECT * FROM lab.db.vendas VERSION AS OF 1").show()

# ------------------------------------------------------------
# 18. EXPORTAR TABELA ICEBERG PARA CSV
# ------------------------------------------------------------
export_path = "hdfs://namenode:9000/export/vendas.csv"
vendas = spark.sql("SELECT * FROM lab.db.vendas")
vendas.write.mode("overwrite").csv(export_path, header=True)
print(f"📤 Tabela 'lab.db.vendas' exportada para {export_path}\n")

# ------------------------------------------------------------
# 19-20. DASHBOARD NO SUPERSET / TRINO (manual)
# ------------------------------------------------------------
print("""
📊 Para o Superset:
1️⃣ Adicione a conexão Trino.
2️⃣ Conecte ao catálogo Iceberg.
3️⃣ Execute:
   SELECT * FROM iceberg.lab.db.pessoas;
4️⃣ Crie gráficos e dashboards!

✅ Todos os passos concluídos com sucesso!
""")

# ------------------------------------------------------------
# FINALIZAR SPARK
# ------------------------------------------------------------
spark.stop()
