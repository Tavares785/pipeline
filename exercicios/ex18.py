from pyspark.sql import SparkSession

def ex18_merge_into(spark: SparkSession) -> None:
    """
    Executa MERGE INTO em lab.db.vendas atualizando valores existentes e inserindo novos.
    """
    # Cria um DataFrame temporário com dados de atualização/inserção
    merge_data = [
        (1, 180.0, 2022),  # Atualiza registro existente
        (5, 350.0, 2024)   # Novo registro a ser inserido
    ]
    columns = ["id", "valor", "ano"]
    df_merge = spark.createDataFrame(merge_data, columns)
    df_merge.createOrReplaceTempView("novas_vendas")

    # Executa o MERGE INTO
    spark.sql("""
        MERGE INTO lab.db.vendas AS t
        USING novas_vendas AS s
        ON t.id = s.id
        WHEN MATCHED THEN
            UPDATE SET t.valor = s.valor, t.ano = s.ano
        WHEN NOT MATCHED THEN
            INSERT (id, valor, ano) VALUES (s.id, s.valor, s.ano)
    """)
