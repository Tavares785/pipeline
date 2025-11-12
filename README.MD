# 🚀 Ambiente Big Data

Este ambiente Docker Compose levanta um **mini cluster de Big Data** com os seguintes componentes:

- **HDFS (Hadoop Distributed File System)**: Armazenamento distribuído de dados.
- **Spark**: Motor de processamento distribuído (modo standalone).
  - Spark Master + 1 Worker.
- **PostgreSQL**: Banco de dados do metastore do Hive.
- **Superset**: Plataforma de visualização de dados.

O ambiente é modular — você pode adicionar outros serviços facilmente (Kafka, Airflow, Zeppelin, etc.).

## 🧩 Estrutura do projeto

```
ambiente_bigdata/
├── docker-compose.yml
└── README.md
```

Principais volumes criados:
- `pgdata`: dados do metastore do Hive (Postgres)
- `hdfs-namenode` e `hdfs-datanode`: dados do HDFS
- `hive-warehouse`: diretório `/user/hive/warehouse`

---

## ⚙️ Como subir o ambiente

### 1. Subir os containers
```bash
docker compose up -d
```

### 2. Verificar status
```bash
docker compose ps
```

### 3. Acompanhar logs
```bash
docker compose logs -f
```

### 4. Parar o ambiente
```bash
docker compose down
```

### Para limpar volumes e dados persistidos:
```bash
docker compose down -v
docker-compose down --rmi all 
```

---

## 🌐 Portas e interfaces

| Serviço        | Porta | Descrição |
|----------------|-------|------------|
| Spark Master UI | 8080 | Interface Web do Spark Master |
| Spark Worker UI | 8081 | Interface Web do Spark Worker |
| Hadoop NameNode UI | 9870 | Monitoramento do HDFS |
| PostgreSQL | 5432 | Banco de dados do metastore |

---
### Criação do usuário admin do Superset
```bash
docker exec -it superset superset db upgrade
docker exec -it superset superset init
docker exec -it superset superset fab create-admin --username admin --password admin --firstname Superset --lastname Admin --email admin@superset.com
docker-compose restart superset
```
String de conexão no Superset:
```
hive://spark-thrift:10000/default
docker exec -it --user root superset bash
---

## ⚡ Testando o Spark

### Acessar o container do Spark Master
```bash
docker exec -it spark-master /bin/bash
```

### Rodar o shell do Spark
```bash
spark-shell --master spark://spark-master:7077
```

### Depois que o cluster estiver de pé, executa:
```bash
docker exec -it namenode bash
hdfs dfs -mkdir -p /user/spark
hdfs dfs -chown -R jovyan:supergroup /user/spark
hdfs dfs -chmod -R 777 /user/spark
```

## Dicas
Dentro de um notebook Python (PySpark):

```python
from pyspark.sql import SparkSession

print("Tentando criar a sessão Spark e conectar ao HDFS...")
# Conecta ao Spark Master e configura o acesso ao HDFS
spark = SparkSession.builder \
    .appName("SparkHDFS") \
    .getOrCreate()

print("Sessão Spark criada com sucesso!")

# --- Teste de Leitura/Escrita no HDFS ---

# Cria um pequeno DataFrame
data = [("Alice", 10), ("Bob", 20), ("Charlie", 30)]
columns = ["name", "value"]
df = spark.createDataFrame(data, columns)

# Define o caminho no HDFS
hdfs_path = "hdfs://namenode:9000/user/spark/data/final_test"

# Escreve o DataFrame no HDFS
# O Spark cria o caminho e o arquivo Parquet
df.write.mode("overwrite").parquet(hdfs_path)
print(f"Dados escritos no HDFS em: {hdfs_path}")

# Lê os dados de volta para verificar
df_read = spark.read.parquet(hdfs_path)
print("\nDados lidos do HDFS:")
df_read.show()

spark.stop()
print("Teste concluído. O ambiente Spark/HDFS está funcionando!")
```