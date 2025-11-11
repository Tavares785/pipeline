# Exercícios de Big Data com Spark + Iceberg + Hive Metastore + HDFS

Este conjunto de exercícios foi desenvolvido para ser executado dentro do ambiente `lab` (Jupyter + Spark + Iceberg + HDFS + Hive Metastore).  
Todos os exercícios devem ser realizados no Jupyter Notebook dentro do container.

---

### **1. Criar um DataFrame simples**
Crie um DataFrame com três linhas e duas colunas (`id`, `nome`) e mostre seu conteúdo.

---

### **2. Salvar DataFrame no HDFS como CSV**
Crie um DataFrame e salve em `hdfs://namenode:9000/data/ex1.csv` no formato CSV.

---

### **3. Ler CSV do HDFS**
Leia o arquivo salvo no exercício anterior usando `spark.read.csv()` e exiba o DataFrame.

---

### **4. Criar namespace Iceberg**
Crie um namespace chamado `lab.db` no catálogo Iceberg.

```sql
CREATE NAMESPACE IF NOT EXISTS lab.db;
```

---

### **5. Criar tabela Iceberg**
Crie uma tabela Iceberg `lab.db.pessoas (id INT, nome STRING)` usando SQL.

---

### **6. Inserir dados na tabela Iceberg**
Insira 3 valores manualmente usando SQL `INSERT INTO`.

---

### **7. Ler tabela Iceberg**
Faça uma query `SELECT * FROM lab.db.pessoas` e exiba o resultado.

---

### **8. Contar registros**
Conte quantos registros existem na tabela `lab.db.pessoas`.

---

### **9. Atualizar um registro**
Atualize um nome usando Iceberg SQL:
```sql
UPDATE lab.db.pessoas SET nome = 'Alice Silva' WHERE nome = 'Alice';
```

---

### **10. Deletar um registro**
Remova uma linha da tabela usando `DELETE FROM`.

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

---

### **12. Inserir dados particionados**
Insira dados variando o valor de `ano`.

---

### **13. Consultar apenas um particionamento**
Leia somente as vendas do ano 2023.

---

### **14. Ver metadados da tabela**
Use:
```sql
DESCRIBE HISTORY lab.db.vendas;
DESCRIBE DETAIL lab.db.vendas;
```

---

### **15. Criar tabela Iceberg a partir de DataFrame**
Crie um DataFrame artificial e grave diretamente com:
```python
df.writeTo("lab.db.tabela_df").createOrReplace()
```

---

### **16. Converter tabela para Iceberg**
Crie uma tabela Parquet simples e converta para Iceberg usando:
```sql
ALTER TABLE ... SET TBLPROPERTIES ('format-version'='2');
```

---

### **17. Leitura incremental (Time Travel)**
Volte para uma versão anterior da tabela:
```sql
SELECT * FROM lab.db.vendas VERSION AS OF 1;
```

---

### **18. Exportar tabela Iceberg para CSV**
Leia a tabela Iceberg e salve para `hdfs://namenode:9000/export/vendas.csv`.

---

### **19. Criar um Dashboard no Superset**
1. Adicione o banco Trino no Superset.
2. Conecte ao catálogo Iceberg.
3. Crie uma visualização simples.

---

### **20. Ler tabela Iceberg via Trino (opcional)**
No Superset ou CLI do Trino:
```sql
SELECT * FROM iceberg.lab.db.pessoas;
```

---

## Dica Final
Se algo der erro, confira:
- Metastore está ativo (`docker logs hive-metastore`)
- HDFS acessível (`hdfs dfs -ls /`)
- Spark com catálogo configurado certo.

Bom estudo! 🚀