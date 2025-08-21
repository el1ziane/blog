---
slug: pyspark
title: Delta Lake + MinIO + Docker + PySpark
date: 2025-02-27
authors: Eliziane
tags: [delta lake, MinIO, docker, PySpark]
keywords: [delta lake, MinIO, docker, PySpark]
---
<!-- truncate -->

# Delta Lake + MinIO + Docker + PySpark

---

Links:
[https://delta.io/learn/getting-started/](https://delta.io/learn/getting-started/)
[https://www.datacamp.com/pt/tutorial/delta-lake](https://www.datacamp.com/pt/tutorial/delta-lake)

[https://datawaybr.medium.com/como-sair-do-zero-no-delta-lake-em-apenas-uma-aula-d152688a4cc8](https://datawaybr.medium.com/como-sair-do-zero-no-delta-lake-em-apenas-uma-aula-d152688a4cc8)

---

Procedimento inicial para utilizar o delta lake com MinIO

- O que é Delta Lake
    
    **Delta Lake** é um framework de **armazenamento transacional** construído sobre o **Apache Spark** e projetado para melhorar a confiabilidade, qualidade e desempenho do **Data Lake**. Ele resolve problemas comuns em lagos de dados tradicionais, como **arquivos corrompidos, falta de transações ACID e gerenciamento de versões dos dados** (viagem no tempo).
    
- O que é MinIO
    
    é um armazenamento **compatível com a API S3 da AWS**, o que permite simular um ambiente similar ao que rodaria na nuvem
    
- Passo 1
    
    Criar uma pasta para desenvolver o projetinho
    Ex: dev/spark-docker
    

Para facilitar é possível abrir essa pasta diretamente no VsCode, em seguida criar o arquivo: docker-compose.yml e colocar o seguinte código:

```python
version: "3.9"

services:
  spark-master:
    image: bitnami/spark:latest
    container_name: spark-master
    hostname: spark-master
    ports:
      - "8080:8080" # UI do Spark Master
      - "7077:7077" # Porta do Spark Master
    user: root
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_HOST=spark-master
    volumes:
      - ./app:/opt/spark/app:rw
      - ./app/libs:/opt/spark/libs # Volume para os JARs
    networks:
      - spark-network

  spark-worker:
    image: bitnami/spark:latest
    container_name: spark-worker
    hostname: spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    networks:
      - spark-network

  minio:
    image: minio/minio
    container_name: minio
    ports:
      - "9000:9000"
      - "9090:9090"
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: admin123
    command: server /data --console-address ":9090"
    volumes:
      - minio-data:/data
    networks:
      - spark-network  

  python:
    build: .
    container_name: python
    hostname: python
    working_dir: /app
    volumes:
      - ./app:/app
    networks:
      - spark-network
    depends_on:
      - spark-master
      - minio
    entrypoint: ["tail", "-f", "/dev/null"]

volumes:
  minio-data:

networks:
  spark-network:
    driver: bridge

```

Com isso será possível criar os container do MinIO, Spark e Python com suas respectivas imagens.
Um adendo para o Python, que não esta instalado localmente e que usará build do Dockerfile para instalar o pySpark toda vez que o container iniciar, pois ele usa a imagem bitnami/spark:latest  que já vem com o Java e Spark instalados:

```jsx
FROM bitnami/spark:latest

USER root

RUN pip install pyspark==3.5.3 delta-spark==3.3.0

WORKDIR /app

VOLUME ["/app"]

CMD ["python", "/app/teste_delta.py"]

```

Se atentar com a compatibilidade das versões do PySpark e do Delta lake, pois a não compatibilidade pode causar diversos erros na execução das bibliotecas.

Outro adendo é com relacao a seguinte parte do código

```jsx
networks:
  spark-network:
    driver: bridge
```

Essa parte define a network dos serviços e para evitar erro todos devem utilizar a mesma network que é spark-network.
abaixo estão alguns comandos para testar e realizar verificações da network
Verificar se todos os contêineres estão na mesma rede:

```jsx
docker network inspect spark-network
```

Recriar os containers e a rede:
O primeiro comando vai remover os contêineres existentes, volumes e órfãos (caso haja contêineres antigos que não estão mais sendo utilizados), e o segundo vai iniciar os contêineres novamente, criando a rede `spark-network`.

```jsx
docker-compose down --volumes --remove-orphans
```

Criar rede manualmente

```jsx
docker network create spark-network
```

Verificar redes existentes:

```jsx
docker network ls
```

Remover rede:

```jsx
docker network rm spark-network
```

verificar em quais redes os contêineres estão conectados:

```jsx
docker ps --format '{{.Names}}: {{.Networks}}'
```

Após criar a rede corretamente é necessário subir os containers novamente:

```jsx
docker-compose up -d
```

Com esse comando os containers  definidos no arquivo docker serão iniciados.

Caso os comandos e alterações na rede não funcione pode ser necessário reiniciar o docker

```jsx
sudo systemctl restart docker
```

```jsx
docker-compose down
docker-compose up
```

Quando realizar uma alteração no docker compose execute os seguintes comandos para reiniciar os containers para aplicar as alterações

```jsx
docker-compose down
docker-compose up --build -d
```

Reconstruir a imagem 

```jsx
docker build -t spark-docker .
```

Com a configurações feitas, vamos testar com um arquivo de teste utilizando pySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaLakeMinIO") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()
#spark.sparkContext.setLogLevel("DEBUG")

try:
 
    data = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

    data.write.format("delta").mode("overwrite").save("s3a://meu-bucket/delta-table")

    print("✅ Dados salvos com sucesso no Delta Lake no MinIO!")
except Exception as e:
    print(f"Erro ao salvar no Delta Lake: {e}")
finally:
    
    spark.stop()

```

A parte inicial do código irá importar o pySpark em seguida define o Delta lake o endereço de comunicação com o MinIO e as credenciais de acesso,
para acessar a simulação da S3 da Aws o MinIO acesse: [http://localhost:9090](http://localhost:9090/).

O código realiza a criação de um data frame com nome e id e em seguida formata para o delta lake  e salva no caminho s3a://meu-bucket/delta-table

**Dica:** No MinIO, é necessário criar o bucket manualmente antes de salvar os dados.

Então como ele esta configurando para salvar no “meu-bucket” é preciso criar o esse bucket no MinIO (talvez definir o acesso para público).

Para executar o código pyhon é necessário entrar no container

```jsx
docker exec -it python bash
```

Em seguida executar o script

```jsx
python /app/teste_delta.py
```

Adicionando novos dados

```python
delta_table_path = "s3a://meu-bucket/delta-table"

delta_table = DeltaTable.forPath(spark, delta_table_path)

new_data = spark.createDataFrame([(11, "Ana Clara"), (2, "Bob Antonio Fernandes")], ["id", "name"]).dropDuplicates(["id"])

if not DeltaTable.isDeltaTable(spark, delta_table_path):
    new_data.write.format("delta").mode("overwrite").save(delta_table_path)
else:
    print("✅ A tabela Delta já existe. Prosseguindo com o merge...")

print("📊 Dados atuais antes do merge:")
delta_table.toDF().show()

```

Aqui utilizando o merge do delta ele irá fazer uma comparação de acordo com a chave id dos dados já inseridos com os novos. Caso seja dados novos ele irá adicionar, caso a chave id corresponda a algum id da tabela dado ele irá fazer o update desse dado nesse sendo de `Alice` para `Ana Alice` e `Bob` `Antonio` para `Bob Antonio Fernandes`.  Já o id 5 com o José será um novo dado introduzido na tabela.

```python
delta_table = DeltaTable.forPath(spark, delta_table_path)
(
     delta_table.alias("dados_atuais")
    .merge(
        new_data.alias("novos_dados"),
        "dados_atuais.id = novos_dados.id"

    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

![DF](/img/project/sparkdocker.png "Spark")

![DF](/img/project/sparkdocker1.png "Spark")
O seguinte código utiliza a função history do delta que apresenta o histórico de tudo o que foi feito na tabela (como: WRITE, DELETE, MERGE) em cada versão da tabela. Sendo possível acessar  versões anteriores de uma tabela (Conceito de viagem no tempo).

```python
df_delta = delta_table.history()
(
    df_delta
    .select("version", "timestamp", "operation", "operationMetrics")
    .show(truncate=False, vertical=True)
)
```

Viagem no tempo - visualizando tabelas de diferentes versões
Ex com Versão 0:
```python
(
    spark
    .read
    .format("delta")
    .option("versionAsOf", 8)
    .load("s3a://meu-bucket/delta-table")
    .show()
)
```

