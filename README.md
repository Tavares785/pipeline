# Ambiente Big Data - Spark + Iceberg + HDFS + Hive + Trino + Superset

## Serviços incluídos:

- **Jupyter Lab** (porta 8888) - Notebooks com Spark + Iceberg
- **Spark Master** (porta 8080) - Interface do Spark
- **HDFS** (porta 9870) - Sistema de arquivos distribuído
- **Hive Metastore** (porta 9083) - Catálogo de metadados
- **Hive Server** (porta 10000) - Servidor Hive
- **Trino** (porta 8082) - Engine de consulta SQL
- **Superset** (porta 8088) - Dashboard e visualização
- **PostgreSQL** (porta 5432) - Banco de dados para metastore

## Como usar:

### 1. Iniciar ambiente:

```bash
docker-compose up -d
```

### 2. Acessar serviços:

- Jupyter: http://localhost:8888
- Spark UI: http://localhost:8080
- HDFS: http://localhost:9870
- Trino: http://localhost:8082
- Superset: http://localhost:8088 (admin/admin)

### 3. Executar exercícios:

Os exercícios estão na pasta `notebooks/exercicios/`
Execute-os no Jupyter Lab na ordem numérica.

### 4. Parar ambiente:

```bash
docker-compose down
```

## Estrutura:

```
TFGustavo/
├── docker-compose.yml
├── Dockerfile
├── hadoop.env
├── start.sh
├── trino/
│   ├── config.properties
│   ├── node.properties
│   ├── jvm.config
│   └── catalog/
│       └── iceberg.properties
└── notebooks/
    └── exercicios/
        ├── exercicio_1.py
        ├── ...
        └── exercicio_20.sql
```

## Versões utilizadas:

- Hadoop: 3.2.1
- Spark: 3.3.0
- Hive: 2.3.2
- PostgreSQL: 11
- Trino: latest
- Superset: latest
