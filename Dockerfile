FROM jupyter/pyspark-notebook:spark-3.3.0

USER root

# Instalar dependências
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER jovyan

# Instalar bibliotecas Python
RUN pip install --no-cache-dir \
    pyiceberg \
    trino \
    sqlalchemy-trino \
    pyhive \
    thrift \
    sasl

# Baixar JARs necessários
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/iceberg-spark-runtime-3.3_2.12-1.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.3.1/iceberg-spark-runtime-3.3_2.12-1.3.1.jar && \
    wget -O /opt/spark/jars/postgresql-42.5.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

# Configurar Spark
ENV SPARK_OPTS="--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.spark_catalog.type=hive \
--conf spark.sql.catalog.spark_catalog.uri=thrift://hive-metastore:9083 \
--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000"

WORKDIR /home/jovyan/work