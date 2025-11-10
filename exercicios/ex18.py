from pyspark.sql import SparkSession

def ex18_merge_into(spark: SparkSession) -> None:
    """
    Executa MERGE INTO em lab.db.vendas atualizando valores.
    """
    atualizacoes = [
        (1, "Produto A", 15, 2024),
        (4, "Produto D", 5, 2025) 
    ]

    df_origem = spark.createDataFrame(atualizacoes, ["id", "produto", "quantidade", "ano"])
    df_origem.createOrReplaceTempView("updates")

    spark.sql("""
        MERGE INTO lab.db.vendas AS destino
        USING updates AS origem
        ON destino.id = origem.id
        WHEN MATCHED THEN
          UPDATE SET
            destino.produto = origem.produto,
            destino.quantidade = origem.quantidade,
            destino.ano = origem.ano
        WHEN NOT MATCHED THEN
          INSERT (id, produto, quantidade, ano)
          VALUES (origem.id, origem.produto, origem.quantidade, origem.ano)
    """)
