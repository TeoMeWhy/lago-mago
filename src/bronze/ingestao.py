# Databricks notebook source
# DBTITLE 1,Imports
import delta
import sys

sys.path.insert(0, "../lib/")

import utils

# COMMAND ----------

# DBTITLE 1,SETUP
catalog = "bronze"
schema = "upsell"

# tablename = "transactions"
# id_field = "idTransaction"
# timestamp_field = "modified_date"

tablename = dbutils.widgets.get("tablename")
id_field = dbutils.widgets.get("id_field")
timestamp_field = dbutils.widgets.get("timestamp_field")

df_schema = utils.import_schema(tablename)

# COMMAND ----------

# DBTITLE 1,Ingestão do Full Load
if not utils.table_exists(spark, catalog, schema, tablename):

    print("Tabela não existente, criando...")

    df_full = (spark.read
                    .format("parquet")
                    .schema(df_schema)
                    .load(f"/Volumes/raw/upsell/full_load/{tablename}/"))

    (df_full.coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
else:
    print("Tabela já existente, ignorando full-load")

# COMMAND ----------

bronze = delta.DeltaTable.forName(spark, f"{catalog}.{schema}.{tablename}")

# COMMAND ----------

# DBTITLE 1,Leitura do CDC

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


df_stream = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "parquet")
                #   .option("cloudFiles.maxFilesPerTrigger", 500)
                  .schema(df_schema)
                  .load(f"/Volumes/raw/upsell/cdc/{tablename}/"))


stream = (df_stream.writeStream
                   .option("checkpointLocation", f"/Volumes/raw/upsell/cdc/{tablename}_checkpoint/")
                   .foreachBatch(lambda df, batchID: upsert(df, bronze))
                   .trigger(availableNow=True))

# COMMAND ----------

start = stream.start()
