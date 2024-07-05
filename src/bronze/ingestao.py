# Databricks notebook source
# DBTITLE 1,Imports
import delta
import sys

sys.path.insert(0, "../lib/")

import utils
import ingestors

# COMMAND ----------

# DBTITLE 1,SETUP
catalog = "bronze"
schemaname = "upsell"

tablename = dbutils.widgets.get("tablename")
id_field = dbutils.widgets.get("id_field")
timestamp_field = dbutils.widgets.get("timestamp_field")

cdc_path = f"/Volumes/raw/{schemaname}/cdc/{tablename}/"
full_load_path = f"/Volumes/raw/{schemaname}/full_load/{tablename}/"
checkpoint_location = f"/Volumes/raw/{schemaname}/cdc/{tablename}_checkpoint/"

# COMMAND ----------

# DBTITLE 1,Ingestão do Full Load
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

# COMMAND ----------

# DBTITLE 1,CDC
ingest_cdc = ingestors.IngestorCDC(spark=spark,
                                   catalog=catalog,
                                   schemaname=schemaname,
                                   tablename=tablename,
                                   data_format="parquet",
                                   id_field=id_field,
                                   timestamp_field=timestamp_field)

stream = ingest_cdc.execute(cdc_path)
