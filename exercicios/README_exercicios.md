# 🚀 Exercícios de Big Data - Implementação Completa

Este repositório contém a implementação completa dos 20 exercícios de Big Data propostos, utilizando um ambiente Docker com Spark, Iceberg, HDFS, Hive Metastore, Trino e Superset.

## 📋 Lista de Exercícios Implementados

### Exercícios Básicos (1-3)
- ✅ **Exercício 1**: Criar DataFrame simples com 3 linhas e 2 colunas
- ✅ **Exercício 2**: Salvar DataFrame no HDFS como CSV
- ✅ **Exercício 3**: Ler CSV do HDFS usando Spark

### Exercícios Iceberg - CRUD (4-10)
- ✅ **Exercício 4**: Criar namespace Iceberg `lab.db`
- ✅ **Exercício 5**: Criar tabela Iceberg `pessoas (id, nome)`
- ✅ **Exercício 6**: Inserir 3 registros na tabela
- ✅ **Exercício 7**: Consultar tabela Iceberg com SELECT
- ✅ **Exercício 8**: Contar registros na tabela
- ✅ **Exercício 9**: Atualizar registro usando UPDATE
- ✅ **Exercício 10**: Deletar registro usando DELETE

### Exercícios Avançados - Particionamento (11-13)
- ✅ **Exercício 11**: Criar tabela particionada por ano
- ✅ **Exercício 12**: Inserir dados com diferentes anos
- ✅ **Exercício 13**: Consultar apenas uma partição específica

### Exercícios de Metadados e Versionamento (14-17)
- ✅ **Exercício 14**: Ver metadados com DESCRIBE HISTORY/DETAIL
- ✅ **Exercício 15**: Criar tabela Iceberg a partir de DataFrame
- ✅ **Exercício 16**: Converter tabela Parquet para Iceberg
- ✅ **Exercício 17**: Time Travel - acessar versões anteriores

### Exercícios de Integração (18-20)
- ✅ **Exercício 18**: Exportar tabela Iceberg para CSV no HDFS
- ✅ **Exercício 19**: Instruções para Dashboard no Superset
- ✅ **Exercício 20**: Queries via Trino para tabelas Iceberg

## 🏗️ Arquitetura do Ambiente

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Jupyter Lab   │    │  Apache Spark   │    │      HDFS       │
│   (Port 8888)   │◄──►│  (Port 8080)    │◄──►│  (Port 9870)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Trino       │    │  Hive Metastore │    │   PostgreSQL    │
│   (Port 8082)   │◄──►│                 │◄──►│   (Port 5432)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │
         ▼
┌─────────────────┐
│   Superset      │
│   (Port 8088)   │
└─────────────────┘
```

## 🚀 Como Executar

### 1. Subir o Ambiente
```bash
# Navegar para o diretório
cd /workspace

# Subir todos os containers
docker-compose up -d

# Verificar status
docker-compose ps
```

### 2. Acessar o Jupyter
```bash
# Abrir no navegador
http://localhost:8888

# Abrir o notebook
notebooks/exercicios_bigdata_completos.ipynb
```

### 3. Executar os Exercícios
- Execute as células sequencialmente
- Cada exercício está documentado e comentado
- Verifique os resultados em cada etapa

## 📊 Dados Criados

### Tabelas Iceberg
1. **lab.db.pessoas** - 3 registros de pessoas
2. **lab.db.vendas** - 9 registros particionados por ano (2022-2024)
3. **lab.db.produtos** - 5 produtos com categorias
4. **lab.db.clientes_iceberg** - 4 clientes convertidos do Parquet

### Arquivos HDFS
- `/data/ex1.csv` - Dados simples exportados
- `/export/vendas.csv` - Tabela vendas exportada
- `/export/produtos.csv` - Tabela produtos exportada

## 🔧 Tecnologias Utilizadas

- **Apache Spark 3.5.1** - Engine de processamento
- **Apache Iceberg** - Formato de tabela com versionamento
- **HDFS** - Sistema de arquivos distribuído
- **Hive Metastore** - Catálogo de metadados
- **Trino** - Engine de query SQL distribuída
- **Apache Superset** - Plataforma de visualização
- **PostgreSQL** - Banco do Metastore
- **Docker & Docker Compose** - Containerização

## 🌐 Interfaces Web

| Serviço | URL | Descrição |
|---------|-----|-----------|
| Jupyter Lab | http://localhost:8888 | Notebooks Python/PySpark |
| Spark Master | http://localhost:8080 | Interface do Spark |
| HDFS NameNode | http://localhost:9870 | Monitoramento HDFS |
| Trino | http://localhost:8082 | Interface de queries |
| Superset | http://localhost:8088 | Dashboards (admin/SenhaForte!123) |

## 📈 Exemplos de Análises

### Vendas por Ano
```sql
SELECT ano, COUNT(*) as vendas, SUM(valor) as receita 
FROM lab.db.vendas 
GROUP BY ano 
ORDER BY ano;
```

### Produtos por Categoria
```sql
SELECT categoria, COUNT(*) as total, AVG(preco) as preco_medio 
FROM lab.db.produtos 
GROUP BY categoria;
```

### Time Travel
```sql
SELECT * FROM lab.db.vendas VERSION AS OF 1;
```

## 🛠️ Comandos Úteis

### Docker
```bash
# Ver logs
docker-compose logs -f

# Parar ambiente
docker-compose down

# Limpar volumes
docker-compose down -v
```

### HDFS
```bash
# Listar arquivos
docker exec -it namenode hdfs dfs -ls /

# Criar diretório
docker exec -it namenode hdfs dfs -mkdir /data
```

### Spark
```bash
# Acessar container
docker exec -it spark-master bash

# Spark Shell
spark-shell --master spark://spark-master:7077
```

## 🎯 Objetivos de Aprendizado

Após completar estes exercícios, você terá experiência prática com:

1. **Processamento Distribuído**: Spark DataFrames e SQL
2. **Armazenamento Moderno**: Formato Iceberg com ACID
3. **Data Lake**: HDFS para armazenamento escalável
4. **Catálogo de Dados**: Hive Metastore para metadados
5. **Query Engine**: Trino para consultas federadas
6. **Visualização**: Superset para dashboards
7. **Versionamento**: Time Travel e snapshots
8. **Particionamento**: Otimização de consultas
9. **ETL**: Extract, Transform, Load pipelines
10. **DevOps**: Docker e orquestração de serviços

## 🔍 Próximos Passos

1. **Explore o Time Travel** - Experimente com diferentes versões
2. **Crie Dashboards** - Use o Superset para visualizações
3. **Otimize Queries** - Teste diferentes estratégias de particionamento
4. **Adicione Dados** - Importe seus próprios datasets
5. **Integre APIs** - Conecte fontes de dados externas

## 📚 Recursos Adicionais

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Trino Documentation](https://trino.io/docs/current/)
- [Apache Superset Documentation](https://superset.apache.org/docs/intro)

---

🎉 **Parabéns por completar todos os 20 exercícios!** 

Este ambiente fornece uma base sólida para projetos de Big Data em produção. Continue explorando e experimentando com diferentes cenários e casos de uso.