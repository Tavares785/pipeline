-- Exercício 20: Ler tabela Iceberg via Trino (opcional)
-- No Superset ou CLI do Trino

-- Conectar ao catálogo Iceberg e consultar tabela pessoas
SELECT * FROM iceberg.lab.db.pessoas;

-- Consultar tabela vendas
SELECT * FROM iceberg.lab.db.vendas;

-- Consulta agregada por ano
SELECT 
    ano,
    COUNT(*) as quantidade_vendas,
    SUM(valor) as total_vendas,
    AVG(valor) as media_vendas
FROM iceberg.lab.db.vendas
GROUP BY ano
ORDER BY ano;

-- Consultar metadados das tabelas
SHOW TABLES FROM iceberg.lab.db;

-- Ver esquema da tabela
DESCRIBE iceberg.lab.db.vendas;