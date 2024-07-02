# Databricks notebook source
import delta

# COMMAND ----------

# DBTITLE 1,Ingestão do Full Load
df_full = spark.read.format("parquet").load("/Volumes/raw/upsell/full_load/customers/")

(df_full.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("bronze.upsell.customers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.upsell.customers
# MAGIC limit 3

# COMMAND ----------

(spark.read
      .format("parquet")
      .load("/Volumes/raw/upsell/cdc/customers/")
      .createOrReplaceTempView("customers"))

query = '''
    SELECT *
    FROM customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY idCustomer ORDER BY modified_date DESC) = 1
'''

df_cdc_unique = spark.sql(query)

# COMMAND ----------

bronze = delta.DeltaTable.forName(spark, "bronze.upsell.customers")

# UPSERT
(bronze.alias("b")
       .merge(df_cdc_unique.alias("d"), "b.idCustomer = d.idCustomer") 
       .whenMatchedDelete(condition = "d.OP = 'D'")
       .whenMatchedUpdateAll(condition = "d.OP = 'U'")
       .whenNotMatchedInsertAll(condition = "d.OP = 'I' OR d.OP = 'U'")
       .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM bronze.upsell.customers
# MAGIC -- where idCustomer = '5f8fcbe0-6014-43f8-8b83-38cf2f4887b3'
