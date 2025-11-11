from pyspark.sql import SparkSession

def ex19_rewrite_data_files(spark: SparkSession) -> None:
    """
    Executa otimização:
    CALL lab.system.rewrite_data_files(table => 'lab.db.vendas');
    """
    # TODO
    raise NotImplementedError

trino://user@trino-host:8080/iceberg/lab/db
connector.name=iceberg
warehouse=hdfs://namenode:9000/warehouse
