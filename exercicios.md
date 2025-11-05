# Exercícios de Big Data com Spark + Iceberg + Hive Metastore + HDFS

Este conjunto de exercícios foi desenvolvido para ser executado dentro do ambiente `lab` (Jupyter + Spark + Iceberg + HDFS + Hive Metastore).  
Todos os exercícios devem ser realizados no Jupyter Notebook dentro do container.

---

### **1. Criar um DataFrame simples**
Crie um DataFrame com três linhas e duas colunas (`id`, `nome`) e mostre seu conteúdo.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Criar DataFrame com dados
data = [
    (1, "Alison"),
    (2, "Medeiros"),
    (3, "joao")
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)


df.show()
```

---

### **2. Salvar DataFrame no HDFS como CSV**
Crie um DataFrame e salve em `hdfs://namenode:9000/data/ex1.csv` no formato CSV.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Criar DataFrame com dados
data = [
    (1, "Alison"),
    (2, "Medeiros"),
    (3, "joao")
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)

# Salvar DataFrame no HDFS como CSV
hdfs_path = "hdfs://namenode:9000/data/ex1.csv"
df.write.mode("overwrite").option("header", "true").csv(hdfs_path)

print(f"DataFrame salvo com sucesso em {hdfs_path}")
```

---

### **3. Ler CSV do HDFS**
Leia o arquivo salvo no exercício anterior usando `spark.read.csv()` e exiba o DataFrame.

```python
# Ler CSV do HDFS
hdfs_path = "hdfs://namenode:9000/data/ex1.csv"
df = spark.read.option("header", "true").csv(hdfs_path)

# Exibir o DataFrame
df.show()

# Mostrar o schema do DataFrame
df.printSchema()
```

---

### **4. Criar namespace Iceberg**
Crie um namespace chamado `lab.db` no catálogo Iceberg.

```sql
CREATE NAMESPACE IF NOT EXISTS lab.db;
```

```python
# Executar o comando SQL para criar o namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.db")

# Verificar se o namespace foi criado
spark.sql("SHOW NAMESPACES IN lab").show()
```

---

### **5. Criar tabela Iceberg**
Crie uma tabela Iceberg `lab.db.pessoas (id INT, nome STRING)` usando SQL.

```sql
CREATE TABLE IF NOT EXISTS lab.db.pessoas (
    id INT,
    nome STRING
) USING ICEBERG;
```

```python
# Executar o comando SQL para criar a tabela Iceberg
spark.sql("""
    CREATE TABLE IF NOT EXISTS lab.db.pessoas (
        id INT,
        nome STRING
    ) USING ICEBERG
""")

# Verificar se a tabela foi criada
spark.sql("SHOW TABLES IN lab.db").show()
```

---

### **6. Inserir dados na tabela Iceberg**
Insira 3 valores manualmente usando SQL `INSERT INTO`.

```sql
INSERT INTO lab.db.pessoas (id, nome) VALUES
    (1, 'Alison'),
    (2, 'Medeiros'),
    (3, 'joao');
```

```python
# Executar o comando SQL para inserir dados
spark.sql("""
    INSERT INTO lab.db.pessoas (id, nome) VALUES
        (1, 'Alison'),
        (2, 'Medeiros'),
        (3, 'joao')
""")

# Verificar os dados inseridos
spark.sql("SELECT * FROM lab.db.pessoas").show()
```

---

### **7. Ler tabela Iceberg**
Faça uma query `SELECT * FROM lab.db.pessoas` e exiba o resultado.

```sql
SELECT * FROM lab.db.pessoas;
```

```python
# Ler e exibir os dados da tabela Iceberg
df = spark.sql("SELECT * FROM lab.db.pessoas")
df.show()

# Mostrar o schema
df.printSchema()
```

---

### **8. Contar registros**
Conte quantos registros existem na tabela `lab.db.pessoas`.

```sql
SELECT COUNT(*) AS total_registros FROM lab.db.pessoas;
```

```python
# Contar registros usando SQL
result = spark.sql("SELECT COUNT(*) AS total_registros FROM lab.db.pessoas")
result.show()

# Alternativa: usar DataFrame
df = spark.sql("SELECT * FROM lab.db.pessoas")
total = df.count()
print(f"Total de registros: {total}")
```

---

### **9. Atualizar um registro**
Atualize um nome usando Iceberg SQL:
```sql
UPDATE lab.db.pessoas SET nome = 'Alice Silva' WHERE nome = 'Alice';
```

---

### **10. Deletar um registro**
Remova uma linha da tabela usando `DELETE FROM`.

```sql
DELETE FROM lab.db.pessoas WHERE id = 3;
```

```python
# Executar o comando SQL para deletar um registro
spark.sql("DELETE FROM lab.db.pessoas WHERE id = 3")

# Verificar os registros restantes
spark.sql("SELECT * FROM lab.db.pessoas").show()
```

---

### **11. Criar tabela particionada**
Crie uma tabela Iceberg com partição por ano:
```sql
CREATE TABLE lab.db.vendas (
  id INT,
  valor DOUBLE,
  ano INT
) USING ICEBERG
PARTITIONED BY (ano);
```

```python
# Executar o comando SQL para criar a tabela particionada
spark.sql("""
    CREATE TABLE lab.db.vendas (
        id INT,
        valor DOUBLE,
        ano INT
    ) USING ICEBERG
    PARTITIONED BY (ano)
""")

# Verificar se a tabela foi criada
spark.sql("SHOW TABLES IN lab.db").show()

# Verificar a estrutura da tabela
spark.sql("DESCRIBE lab.db.vendas").show()
```

---

### **12. Inserir dados particionados**
Insira dados variando o valor de `ano`.

```sql
INSERT INTO lab.db.vendas (id, valor, ano) VALUES
    (1, 1000.50, 2022),
    (2, 1500.75, 2022),
    (3, 2000.00, 2023),
    (4, 2500.25, 2023),
    (5, 3000.50, 2024),
    (6, 3500.00, 2024);
```

```python
# Executar o comando SQL para inserir dados particionados
spark.sql("""
    INSERT INTO lab.db.vendas (id, valor, ano) VALUES
        (1, 1000.50, 2022),
        (2, 1500.75, 2022),
        (3, 2000.00, 2023),
        (4, 2500.25, 2023),
        (5, 3000.50, 2024),
        (6, 3500.00, 2024)
""")

# Verificar os dados inseridos
spark.sql("SELECT * FROM lab.db.vendas ORDER BY ano, id").show()
```

---

### **13. Consultar apenas um particionamento**
Leia somente as vendas do ano 2023.

```sql
SELECT * FROM lab.db.vendas WHERE ano = 2023;
```

```python
# Consultar apenas as vendas do ano 2023 (um único particionamento)
df_2023 = spark.sql("SELECT * FROM lab.db.vendas WHERE ano = 2023")
df_2023.show()

# Verificar quantos registros foram retornados
print(f"Total de vendas em 2023: {df_2023.count()}")
```

---

### **14. Ver metadados da tabela**
Use:
```sql
DESCRIBE HISTORY lab.db.vendas;
DESCRIBE DETAIL lab.db.vendas;
```

```python
# Ver o histórico de versões da tabela
print("=== HISTÓRICO DA TABELA ===")
spark.sql("DESCRIBE HISTORY lab.db.vendas").show(truncate=False)

# Ver detalhes e metadados da tabela
print("\n=== DETALHES DA TABELA ===")
spark.sql("DESCRIBE DETAIL lab.db.vendas").show(truncate=False)
```

---

### **15. Criar tabela Iceberg a partir de DataFrame**
Crie um DataFrame artificial e grave diretamente com:
```python
df.writeTo("lab.db.tabela_df").createOrReplace()
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Criar DataFrame artificial
data = [
    (1, "Produto A", 100.50),
    (2, "Produto B", 200.75),
    (3, "Produto C", 300.00),
    (4, "Produto D", 400.25)
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("preco", DoubleType(), True)
])

df = spark.createDataFrame(data, schema=schema)

# Exibir o DataFrame antes de gravar
print("DataFrame criado:")
df.show()

# Gravar diretamente como tabela Iceberg
df.writeTo("lab.db.tabela_df").createOrReplace()

# Verificar se a tabela foi criada
print("\nTabela criada com sucesso!")
spark.sql("SELECT * FROM lab.db.tabela_df").show()
```

---

### **16. Converter tabela para Iceberg**
Crie uma tabela Parquet simples e converta para Iceberg usando:
```sql
ALTER TABLE ... SET TBLPROPERTIES ('format-version'='2');
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Criar DataFrame para exemplo
data = [
    (1, "Cliente A"),
    (2, "Cliente B"),
    (3, "Cliente C")
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True)
])

df = spark.createDataFrame(data, schema=schema)

# Criar tabela Parquet simples
spark.sql("""
    CREATE TABLE IF NOT EXISTS lab.db.clientes_parquet (
        id INT,
        nome STRING
    ) USING PARQUET
""")

# Inserir dados na tabela Parquet
df.write.mode("overwrite").saveAsTable("lab.db.clientes_parquet")

# Verificar dados na tabela Parquet
print("Tabela Parquet criada:")
spark.sql("SELECT * FROM lab.db.clientes_parquet").show()

# Converter tabela Parquet para Iceberg
spark.sql("""
    ALTER TABLE lab.db.clientes_parquet 
    SET TBLPROPERTIES ('format-version'='2', 'write.format.default'='iceberg')
""")

print("\nTabela convertida para Iceberg!")
spark.sql("SELECT * FROM lab.db.clientes_parquet").show()
```

---

### **17. Leitura incremental (Time Travel)**
Volte para uma versão anterior da tabela:
```sql
SELECT * FROM lab.db.vendas VERSION AS OF 1;
```

```python
# Verificar o histórico de versões da tabela
print("=== HISTÓRICO DE VERSÕES ===")
history = spark.sql("DESCRIBE HISTORY lab.db.vendas")
history.show(truncate=False)

# Ver a versão atual da tabela
print("\n=== VERSÃO ATUAL ===")
spark.sql("SELECT * FROM lab.db.vendas").show()

# Voltar para uma versão anterior usando Time Travel
print("\n=== VERSÃO ANTERIOR (VERSION AS OF 1) ===")
df_version_1 = spark.sql("SELECT * FROM lab.db.vendas VERSION AS OF 1")
df_version_1.show()

# Alternativa: usar TIMESTAMP AS OF para voltar a uma data específica
# spark.sql("SELECT * FROM lab.db.vendas TIMESTAMP AS OF '2024-01-01 00:00:00'").show()
```

---

### **18. Exportar tabela Iceberg para CSV**
Leia a tabela Iceberg e salve para `hdfs://namenode:9000/export/vendas.csv`.

```python
# Ler a tabela Iceberg
df_vendas = spark.sql("SELECT * FROM lab.db.vendas")

# Exibir os dados antes de exportar
print("Dados da tabela Iceberg:")
df_vendas.show()

# Exportar para CSV no HDFS
hdfs_export_path = "hdfs://namenode:9000/export/vendas.csv"
df_vendas.write.mode("overwrite").option("header", "true").csv(hdfs_export_path)

print(f"\nTabela Iceberg exportada com sucesso para {hdfs_export_path}")

# Verificar o arquivo exportado (opcional)
print("\nVerificando arquivo exportado:")
df_verificacao = spark.read.option("header", "true").csv(hdfs_export_path)
df_verificacao.show()
```

---

### **19. Criar um Dashboard no Superset**
1. Adicione o banco Trino no Superset.
2. Conecte ao catálogo Iceberg.
3. Crie uma visualização simples.

**Passos para configurar:**

1. **Adicionar banco de dados Trino no Superset:**
   - Acesse Superset (geralmente em `http://localhost:8088`)
   - Vá em **Settings** → **Database Connections** → **+ Database**
   - Selecione **Trino** como engine
   - Configure a conexão:
     ```
     SQLALCHEMY URI: trino://trino:8080/iceberg
     ```
   - Ou use a string de conexão completa:
     ```
     trino://user:password@trino:8080/iceberg
     ```

2. **Testar conexão e listar tabelas:**
   ```sql
   -- Listar schemas
   SHOW SCHEMAS FROM iceberg;
   
   -- Listar tabelas
   SHOW TABLES FROM iceberg.lab.db;
   
   -- Testar consulta
   SELECT * FROM iceberg.lab.db.pessoas;
   ```

3. **Criar visualização simples:**
   - No Superset, vá em **Charts** → **+ Chart**
   - Selecione a tabela `iceberg.lab.db.vendas`
   - Escolha o tipo de visualização (ex: Table, Bar Chart)
   - Configure os campos:
     - **Query**: 
       ```sql
       SELECT ano, SUM(valor) as total_vendas
       FROM iceberg.lab.db.vendas
       GROUP BY ano
       ORDER BY ano
       ```
   - Salve a visualização

4. **Criar Dashboard:**
   - Vá em **Dashboards** → **+ Dashboard**
   - Adicione as visualizações criadas
   - Organize os componentes no layout

**Exemplo de query para visualização:**
```sql
-- Vendas por ano
SELECT 
    ano,
    COUNT(*) as quantidade,
    SUM(valor) as total_vendas,
    AVG(valor) as media_vendas
FROM iceberg.lab.db.vendas
GROUP BY ano
ORDER BY ano;
```

---

### **20. Ler tabela Iceberg via Trino (opcional)**
No Superset ou CLI do Trino:
```sql
SELECT * FROM iceberg.lab.db.pessoas;
```

**Via CLI do Trino:**

```bash
# Conectar ao Trino CLI (dentro do container)
docker exec -it trino trino

# Ou se o Trino estiver rodando localmente
trino --server localhost:8080 --catalog iceberg
```

**Queries SQL no Trino:**

```sql
-- Listar schemas disponíveis
SHOW SCHEMAS FROM iceberg;

-- Listar tabelas no namespace lab.db
SHOW TABLES FROM iceberg.lab.db;

-- Ler tabela de pessoas
SELECT * FROM iceberg.lab.db.pessoas;

-- Ler tabela de vendas
SELECT * FROM iceberg.lab.db.vendas;

-- Consulta com filtros
SELECT * FROM iceberg.lab.db.vendas WHERE ano = 2023;

-- Agregações
SELECT 
    ano,
    COUNT(*) as total_registros,
    SUM(valor) as total_vendas,
    AVG(valor) as media_vendas
FROM iceberg.lab.db.vendas
GROUP BY ano
ORDER BY ano;
```

**Via Superset:**

1. Conecte-se ao banco Trino configurado
2. Use o SQL Lab para executar queries:
   ```sql
   SELECT * FROM iceberg.lab.db.pessoas;
   ```
3. Ou crie uma visualização usando a tabela `iceberg.lab.db.pessoas`

**Exemplo Python para conectar via Trino (usando trino-python-client):**

```python
# Nota: Este código requer instalação do trino-python-client
# pip install trino

from trino.dbapi import connect

# Conectar ao Trino
conn = connect(
    host='trino',
    port=8080,
    user='admin',
    catalog='iceberg',
    schema='lab.db'
)

# Executar query
cursor = conn.cursor()
cursor.execute("SELECT * FROM iceberg.lab.db.pessoas")
rows = cursor.fetchall()

# Exibir resultados
for row in rows:
    print(row)
```

---

## Dica Final
Se algo der erro, confira:
- Metastore está ativo (`docker logs hive-metastore`)
- HDFS acessível (`hdfs dfs -ls /`)
- Spark com catálogo configurado certo.

Bom estudo! 🚀