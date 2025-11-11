-- Exercício 20: Ler tabela Iceberg via Trino

-- Conectar ao Trino CLI:
-- trino --server localhost:8080 --catalog iceberg --schema lab.db

-- Queries:
SELECT * FROM iceberg.lab.db.pessoas;
SELECT * FROM iceberg.lab.db.vendas;

-- Agregação:
SELECT ano, COUNT(*) as qtd_vendas, SUM(valor) as total_vendas
FROM iceberg.lab.db.vendas 
GROUP BY ano 
ORDER BY ano;