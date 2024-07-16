# Databricks notebook source
# DBTITLE 1,SETUP
import sys

sys.path.insert(0, "../lib")

import utils
import ingestors

tablename = dbutils.widgets.get("tablename")
idfield = dbutils.widgets.get("id_field")
idfield_old = dbutils.widgets.get("id_field_old")

# tablename = "transacoes"
# idfield = "idTransacao"
# idfield_old = "idTransaction"

catalog = "silver"
schemaname = "upsell"

# COMMAND ----------

# DBTITLE 1,INGESTAO FULL-LOAD

remove_checkpoint = False

if not utils.table_exists(spark, "silver", "upsell", tablename):

    print("Criando a tabela", tablename)
    query = utils.import_query(f"{tablename}.sql")
    (spark.sql(query)
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"silver.upsell.{tablename}"))
    
    remove_checkpoint = True

# COMMAND ----------

# DBTITLE 1,Ingestão CDF + Streaming
print("Iniciando CDF...")

ingest = ingestors.IngestorCDF(spark=spark,
                               catalog=catalog,
                               schemaname=schemaname,
                               tablename=tablename,
                               id_field=idfield,
                               idfield_old=idfield_old)

if remove_checkpoint:
    dbutils.fs.rm(ingest.checkpoint_location, True)

stream = ingest.execute()
print("Ok.")
