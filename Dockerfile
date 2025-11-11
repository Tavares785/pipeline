FROM jupyter/pyspark-notebook:latest

USER root

# Install Iceberg dependencies
RUN wget -P /usr/local/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.2/iceberg-spark-runtime-3.4_2.12-1.4.2.jar

# Install additional Python packages
RUN pip install --no-cache-dir \
    pyiceberg \
    trino

USER $NB_UID

# Copy exercises
COPY --chown=$NB_UID:$NB_GID ./exercicios /home/jovyan/work/exercicios/

WORKDIR /home/jovyan/work