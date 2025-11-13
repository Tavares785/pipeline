# 🎓 Exercícios de Big Data - Spark + Iceberg + HDFS

Este diretório contém 20 exercícios práticos de Big Data usando Apache Spark, Apache Iceberg e HDFS.

## 📋 Lista de Exercícios

### Básicos (DataFrame e HDFS)
- **Exercício 01**: Criar DataFrame simples com dados de funcionários
- **Exercício 02**: Salvar DataFrame no HDFS como CSV
- **Exercício 03**: Ler CSV do HDFS
- **Exercício 04**: Criar namespace Iceberg "exercicios"
- **Exercício 05**: Criar tabela Iceberg "funcionarios"
- **Exercício 06**: Inserir dados na tabela Iceberg

### Intermediários (Operações de Tabela)
- **Exercício 07**: Consultar dados da tabela Iceberg
- **Exercício 08**: Atualizar dados específicos
- **Exercício 09**: Deletar registros
- **Exercício 10**: Fazer merge (upsert) de dados
- **Exercício 11**: Criar tabela particionada por departamento
- **Exercício 12**: Inserir dados na tabela particionada
- **Exercício 13**: Consultar tabela particionada
- **Exercício 14**: Demonstrar time travel

### Avançados (Performance e Analytics)
- **Exercício 15**: Criar tabela Iceberg a partir de DataFrame
- **Exercício 16**: Fazer join entre tabelas Iceberg
- **Exercício 17**: Criar view temporária e SQL complexo
- **Exercício 18**: Exportar tabela Iceberg para CSV
- **Exercício 19**: Otimizar tabela (compactação)
- **Exercício 20**: Analytics com agregações complexas

## 🚀 Como Executar

### Pré-requisitos

1. **Ambiente Spark + Iceberg**: 
   - Apache Spark 3.5.x
   - Apache Iceberg 1.4.x
   - Hadoop HDFS
   - Hive Metastore

2. **Dependências Python**:
   ```bash
   pip install -r requirements.txt
   ```

3. **JARs do Iceberg**:
   - Download dos JARs necessários:
     - `iceberg-spark-runtime-3.5_2.12-1.4.2.jar`
     - `iceberg-spark-extensions-3.5_2.12-1.4.2.jar`

### Configuração do Ambiente

#### Opção 1: Docker Compose (Recomendado)
```yaml
# docker-compose.yml
version: '3.8'
services:
  namenode:
    image: apache/hadoop:3.3.6
    ports:
      - "9000:9000"
      - "9870:9870"
    # ... configuração HDFS
  
  hive-metastore:
    image: apache/hive:4.0.0
    ports:
      - "9083:9083"
    # ... configuração Hive
```

#### Opção 2: Configuração Local
```bash
# Configurar variáveis de ambiente
export SPARK_HOME=/path/to/spark
export HADOOP_HOME=/path/to/hadoop
export JAVA_HOME=/path/to/java

# Adicionar JARs do Iceberg ao classpath
export SPARK_CLASSPATH=$SPARK_CLASSPATH:/path/to/iceberg-jars/*
```

### Executando os Exercícios

#### Todos os exercícios principais:
```bash
python run_exercises.py
```

#### Exercício específico:
```bash
python run_exercises.py 1      # Executa exercício 1
python run_exercises.py 15     # Executa exercício 15
```

#### Todos os exercícios (1-20):
```bash
python run_exercises.py all
```

#### Ajuda:
```bash
python run_exercises.py help
```

### Executando exercícios individuais:
```bash
python exercicio_01.py
python exercicio_02.py
# ... etc
```

## 📁 Estrutura dos Arquivos

```
exercicios/
├── config.py              # Configurações compartilhadas
├── run_exercises.py        # Executor principal
├── requirements.txt        # Dependências Python
├── README.md              # Esta documentação
├── exercicio_01.py        # Exercício 1: DataFrame básico
├── exercicio_02.py        # Exercício 2: Salvar CSV
├── exercicio_03.py        # Exercício 3: Ler CSV
├── exercicio_04.py        # Exercício 4: Namespace Iceberg
├── exercicio_05.py        # Exercício 5: Tabela Iceberg
├── exercicio_06.py        # Exercício 6: Inserir dados
├── exercicio_11.py        # Exercício 11: Tabela particionada
├── exercicio_15.py        # Exercício 15: DataFrame para Iceberg
└── exercicio_18.py        # Exercício 18: Exportar CSV
```

## 🔧 Configuração

### Arquivo config.py
Contém configurações compartilhadas:
- URLs do HDFS (`hdfs://namenode:9000`)
- URLs do Hive Metastore (`thrift://hive-metastore:9083`)
- Configuração do Spark com Iceberg
- Dados de exemplo para os exercícios

### Personalização
Para adaptar a seu ambiente, edite `config.py`:
```python
def get_spark_iceberg_config():
    return {
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "hive",
        "spark.sql.catalog.iceberg.uri": "thrift://SEU-HIVE-METASTORE:9083",
        # ... outras configurações
    }
```

## 🐛 Troubleshooting

### Erro: "HDFS connection refused"
- Verifique se o HDFS está rodando na porta 9000
- Confirme a conectividade: `hdfs dfs -ls /`

### Erro: "Hive Metastore connection failed"
- Verifique se o Hive Metastore está rodando na porta 9083
- Confirme a conectividade com `telnet hive-metastore 9083`

### Erro: "Iceberg classes not found"
- Adicione os JARs do Iceberg ao classpath do Spark
- Use `--jars` ao executar spark-submit

### Erro: "PySpark not found"
- Instale PySpark: `pip install pyspark==3.5.0`
- Configure PYTHONPATH para incluir Spark

## 📚 Recursos Adicionais

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Hadoop HDFS Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)

## 🤝 Contribuição

Para adicionar novos exercícios:
1. Crie arquivo `exercicio_XX.py` seguindo o padrão existente
2. Implemente função `main()` que retorna True/False
3. Adicione tratamento de erros adequado
4. Atualize este README.md

## 📄 Licença

Este projeto é para fins educacionais. Use livremente para aprender Big Data com Spark e Iceberg!