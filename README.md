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
