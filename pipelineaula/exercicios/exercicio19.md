# Exercício 19: Criar um Dashboard no Superset

## Passos:

1. **Adicionar conexão Trino:**
   - URL: `trino://trino:8080/iceberg`
   - Username: `admin`

2. **Conectar ao catálogo Iceberg:**
   - Database: `iceberg`
   - Schema: `lab.db`

3. **Criar dataset:**
   - Selecionar tabela `vendas` ou `pessoas`

4. **Criar visualização:**
   - Gráfico de barras com vendas por ano
   - Tabela simples com dados das pessoas