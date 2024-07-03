# Databricks notebook source
# DBTITLE 1,Imports
import delta

def table_exists(catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND tableName = '{table}'")
                .count())
    return count == 1

# COMMAND ----------

# DBTITLE 1,SETUP
catalog = "bronze"
schema = "upsell"
tablename = dbutils.widgets.get("tablename")
id_field = dbutils.widgets.get("id_field")
timestamp_field = dbutils.widgets.get("timestamp_field")

# COMMAND ----------

# DBTITLE 1,Ingestão do Full Load
if not table_exists(catalog, schema, tablename):

    print("Tabela não existente, criando...")

    df_full = spark.read.format("parquet").load(f"/Volumes/raw/upsell/full_load/{tablename}/")

    (df_full.coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
else:
    print("Tabela já existente, ignorando full-load")

# COMMAND ----------

# DBTITLE 1,Leitura do CDC
(spark.read
      .format("parquet")
      .load(f"/Volumes/raw/upsell/cdc/{tablename}/")
      .createOrReplaceTempView(f"view_{tablename}"))

query = f'''
    SELECT *
    FROM view_{tablename}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_field} ORDER BY {timestamp_field} DESC) = 1
'''

df_cdc_unique = spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Escrita CDC
bronze = delta.DeltaTable.forName(spark, f"{catalog}.{schema}.{tablename}")

# UPSERT
(bronze.alias("b")
       .merge(df_cdc_unique.alias("d"), f"b.{id_field} = d.{id_field}") 
       .whenMatchedDelete(condition = "d.OP = 'D'")
       .whenMatchedUpdateAll(condition = "d.OP = 'U'")
       .whenNotMatchedInsertAll(condition = "d.OP = 'I' OR d.OP = 'U'")
       .execute()
)
