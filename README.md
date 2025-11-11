# Como Executar o Ambiente Big Data

## 1. Pré-requisitos
- Docker Desktop instalado
- 8GB RAM disponível
- 10GB espaço em disco

## 2. Iniciar o ambiente
```bash
cd gustavopipiline
docker-compose up -d
```

## 3. Aguardar inicialização (5-10 minutos)
```bash
# Verificar status
docker-compose ps

# Ver logs se necessário
docker-compose logs -f
```

## 4. Acessar serviços

### Jupyter Notebook
- URL: http://localhost:8888
- Token: Verificar nos logs com `docker-compose logs jupyter`
- Pasta: `/work/exercicios` (contém os 20 exercícios)

### HDFS Web UI
- URL: http://localhost:9870

### Trino Web UI  
- URL: http://localhost:8080

## 5. Executar exercícios

### No Jupyter:
1. Acesse http://localhost:8888
2. Navegue para `work/exercicios/`
3. Abra `exercicio1.py`
4. Execute célula por célula

### Exemplo de execução:
```python
# No Jupyter, criar nova célula e executar:
exec(open('/home/jovyan/work/exercicios/exercicio1.py').read())
```

## 6. Parar ambiente
```bash
docker-compose down
```

## 7. Limpar dados (opcional)
```bash
docker-compose down -v
```