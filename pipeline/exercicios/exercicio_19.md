# Exercício 19: Criar um Dashboard no Superset

## Passos para criar dashboard no Superset:

### 1. Adicionar banco Trino no Superset
- Acesse a interface do Superset
- Vá em Settings > Database Connections
- Clique em "+ DATABASE"
- Selecione "Trino" como tipo de banco
- Configure a conexão:
  ```
  Host: trino
  Port: 8080
  Database: iceberg
  Username: admin
  ```

### 2. Conectar ao catálogo Iceberg
- Teste a conexão
- Salve a configuração
- Verifique se as tabelas do catálogo `lab.db` estão visíveis

### 3. Criar visualização simples
- Vá em Charts > + CHART
- Selecione a tabela `lab.db.vendas`
- Escolha um tipo de gráfico (ex: Bar Chart)
- Configure:
  - X-axis: ano
  - Metric: SUM(valor)
- Clique em "RUN QUERY"
- Salve o gráfico

### 4. Criar Dashboard
- Vá em Dashboards > + DASHBOARD
- Adicione o gráfico criado
- Configure layout e filtros
- Salve o dashboard

## Exemplo de query SQL no Superset:
```sql
SELECT ano, SUM(valor) as total_vendas
FROM iceberg.lab.db.vendas
GROUP BY ano
ORDER BY ano;
```