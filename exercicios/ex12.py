from pyspark.sql import SparkSession

def ex12_create_df_table(spark: SparkSession) -> None:
    """
    Cria DataFrame e salva como lab.db.tabela_df via writeTo.
    """
    # TODO
    raise NotImplementedError


INSERT INTO lab.db.vendas VALUES
    (1, 150.75, 2023),
    (2, 299.90, 2024),
    (3, 450.00, 2025);
