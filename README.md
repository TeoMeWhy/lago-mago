# O Lago do Mago

Construção de um Lakehouse completamente do zero!

Seja membro ou sub de nossos canais e assista todos os vídeos deste projeto.

- [YouTube](https://www.youtube.com/playlist?list=PLvlkVRRKOYFTcLehYZ2Bd5hGIcLH0dJHE)
- [Twitch](https://www.twitch.tv/collections/2e8D0Vgd3hf04g)

<img src="https://i.ibb.co/vvzVMnn/Datalake-upsell.png" alt="Datalake-upsell" border="0">

## Sobre

A partir dos dados do nosso sistema de pontos, vamos construir ingestões de dados no Databricks.

DB -> Raw -> Bronze -> Silver -> Silver FS -> Modelo I.A.

### Envio dos dados para o bucket S3

Criamos um script em Python que verifica cada novo registro (ou atualização) que ocorre no banco de dados em produto. Este mesmo script envia os dados de cada tabela para o S3 em formato `.parquet`, simulando um `Change Data Capture` (CDC).

Foi realizda uma carga `full-load` dia 13/06/2024, para o mesmo bucket, em um diretório específico.

A criação deste script foi realizado em algumas lives aleatórias do dia a dia (estamos online todos os dias 9AM na [Twitch](https://twitch.tv/teomewhy).

### Setup Databricks

No primeiro dia de projeto, mostramos como realizar o setup do ambiente do Databricks. Isto é:
- Criação do Workspace + Unity Catalog
- Setup do External Location (S3 em Raw)
- Adição do Volume dos dados em Raw

### Consumo dos dados para Bronze

Seguimos no projeto para realizar as primeiras ingestões de dados.

Criamos nosso primeiro notebook e fizemos a leitura dos dados `full-load` em Raw e salvamos em Bronze.

Algo similar à este script:

```python
df_full = (spark.read
                .format("parquet")
                .load(f"/Volumes/raw/upsell/full_load/{tablename}/"))

(df_full.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
```

Ainda neste mesmo dia, realizamos a ingestão de todos os dados em CDC com Upsert em Delta.

Ou seja, identificamos a última versão válida do dado com base na `primary key` e no campo `modified date` que vem do CDC.

```python
(spark.read
      .format("parquet")
      .load(f"/Volumes/raw/upsell/cdc/{tablename}/")
      .createOrReplaceTempView(f"view_{tablename}"))

query = f'''
    SELECT *
    FROM "view_{tablename}"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY modified_date DESC) = 1
'''

df_cdc_unique = spark.sql(query)

bronze = delta.DeltaTable.forName(spark, f"{catalog}.{schema}.{tablename}")

(bronze.alias("b")
       .merge(df_cdc_unique.alias("d"), f"b.{primary_key} = d.{primary_key}") 
       .whenMatchedDelete(condition = "d.OP = 'D'")
       .whenMatchedUpdateAll(condition = "d.OP = 'U'")
       .whenNotMatchedInsertAll(condition = "d.OP = 'I' OR d.OP = 'U'")
       .execute())
```

Apesar deste código ser funcional, não é muito bacana. Pois a cada nova carga em CDC, todos os arquivos são lidos e processados. No dia seguinte mostramos uma solução interessante  para esta questão, utilizando Spark Streaming (`CloudFiles`).

### Consumo por Streaming (CloudFiles)

Embora a solução anterior em `batch` para `CDC` tenha funcionado, essa não é uma solução muito performática. Uma vez que a cada nova carga, todos os dados na pasta `CDC` serão lidos e processados. existem algumas alternativas para solucionar essa perda de performance, como:

- Particionamento dos dados em pastas de data (yyyy-mm-dd)
- Movimentação dos arquivos lidos para outro diretório/bucket
- Leitura via Streaming

Adotaremos a última opção, realizando a leitura dos dados utilizando Streaming com Apache Spark:

```python
df_stream = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "parquet")
                  .schema(schema)
                  .load(f"/Volumes/raw/upsell/cdc/{tablename}/"))
```

A opção de formato `cloudFiles` é algo específico do Databricks. Você pode utilizar apenas `parquet` caso esteja trabalhando com o Apache Spark Vanilla. Vale ressaltar que para o streaming funcionar, é necessário passar o `schema` dos arquivos a serem lidos.

O próximo passo é realizar a escrita dos dados a partir do Dataframe criado com Streaming:

```python
stream = (df_stream.writeStream
                   .option("checkpointLocation", f"/Volumes/raw/upsell/cdc/{tablename}_checkpoint/")
                   .foreachBatch(lambda df, batchID: upsert(df, bronze))
                   .trigger(availableNow=True))
```

Pontos de destaque:
- `checkpointLocation`:trata-se de uma diretório especial onde o Spark conseguirá identificar a partir de qual arquivo ele deve ler na próxima iteração.
- `.foreachBatch`: definição de como cada batch do streaming será processado, i.e. como lidaremos com os dados.
- `.trigger(availableNow=True))`: garante que após o processamento de todos os arvuiso disponíveis, a stream é encerrada.

Agora, precisamos definir a função `upsert`. Ela seguirá o mesmo racional apresentado na etapa de CDC anteriormente, isto é, consolidação dos dados novos para realizar o merge na tabela já existente em bronze.

```python
def upsert(df, deltatable):
    df.createOrReplaceGlobalTempView(f"view_{tablename}")

    query = f'''
        SELECT *
        FROM global_temp.view_{tablename}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_field} ORDER BY {timestamp_field} DESC) = 1
    '''

    df_cdc = spark.sql(query)

    (deltatable.alias("b")
               .merge(df_cdc.alias("d"), f"b.{id_field} = d.{id_field}") 
               .whenMatchedDelete(condition = "d.OP = 'D'")
               .whenMatchedUpdateAll(condition = "d.OP = 'U'")
               .whenNotMatchedInsertAll(condition = "d.OP = 'I' OR d.OP = 'U'")
               .execute())
```

Ou seja, desta forma, para cada batch lido na stream, realizamos o upsert dos dos em bronze.

### Classes de ingestão

Buscando melhorar ainda mais o nosso código, decidimos construir algumas classes para ingestão desses dados. Isso nos ajudará aplicar esses mesmos métodos e estratégias de ingestão em outros contextos ou necessidades.

#### Classe para carga Full-load

```python
class Ingestor:

    def __init__(self, spark, catalog, schemaname, tablename, data_format):
        self.spark = spark
        self.catalog = catalog
        self.schemaname = schemaname
        self.tablename = tablename
        self.format = data_format
        self.set_schema()

    def set_schema(self):
        self.data_schema = utils.import_schema(self.tablename)
    
    def load(self, path):
        df = (self.spark
                  .read
                  .format(self.format)
                  .schema(self.data_schema)
                  .load(path))
        return df
        
    def save(self, df):
        (df.write
           .format("delta")
           .mode("overwrite")
           .saveAsTable(f"{self.catalog}.{self.schemaname}.{self.tablename}"))
        return True
        
    def execute(self, path):
        df = self.load(path)
        return self.save(df)
```

#### Classe para carga CDC

```python
class IngestorCDC(Ingestor):

    def __init__(self, spark, catalog, schemaname, tablename, data_format, id_field, timestamp_field):
        super().__init__(spark, catalog, schemaname, tablename, data_format)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.set_deltatable()

    def set_deltatable(self):
        tablename = f"{self.catalog}.{self.schemaname}.{self.tablename}"
        self.deltatable = delta.DeltaTable.forName(self.spark, tablename)

    def upsert(self, df):
        df.createOrReplaceGlobalTempView(f"view_{self.tablename}")
        query = f'''
            SELECT *
            FROM global_temp.view_{self.tablename}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) = 1
        '''

        df_cdc = self.spark.sql(query)

        (self.deltatable
             .alias("b")
             .merge(df_cdc.alias("d"), f"b.{self.id_field} = d.{self.id_field}") 
             .whenMatchedDelete(condition = "d.OP = 'D'")
             .whenMatchedUpdateAll(condition = "d.OP = 'U'")
             .whenNotMatchedInsertAll(condition = "d.OP = 'I' OR d.OP = 'U'")
             .execute())

    def load(self, path):
        df = (self.spark
                  .readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", self.format)
                  .schema(self.data_schema)
                  .load(path))
        return df
    
    def save(self, df):
        stream = (df.writeStream
                   .option("checkpointLocation", f"/Volumes/raw/{self.schemaname}/cdc/{self.tablename}_checkpoint/")
                   .foreachBatch(lambda df, batchID: self.upsert(df))
                   .trigger(availableNow=True))
        return stream.start()
```

#### Execução

Com isso em mente, podemos apenas invorcar as classes e realizar sua execução:

```python
if not utils.table_exists(spark, catalog, schemaname, tablename):

    print("Tabela não existente, criando...")

    dbutils.fs.rm(checkpoint_location, True)

    ingest_full_load = ingestors.Ingestor(spark=spark,
                                          catalog=catalog,
                                          schemaname=schemaname,
                                          tablename=tablename,
                                          data_format="parquet")
    
    ingest_full_load.execute(full_load_path)
    print("Tabela criada com sucesso!")
    
else:
    print("Tabela já existente, ignorando full-load")

print("Executando carga cdc...")
ingest_cdc = ingestors.IngestorCDC(spark=spark,
                                   catalog=catalog,
                                   schemaname=schemaname,
                                   tablename=tablename,
                                   data_format="parquet",
                                   id_field=id_field,
                                   timestamp_field=timestamp_field)

stream = ingest_cdc.execute(cdc_path)
print("ok")
```
