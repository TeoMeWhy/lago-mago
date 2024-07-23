# Databricks notebook source
# DBTITLE 1,IMPORTS
import tqdm
import sys
import datetime

sys.path.insert(0, "../lib")

import utils
import ingestors

# COMMAND ----------

# DBTITLE 1,SETUP
catalog = "gold"
schemaname = 'upsell'
tablename = dbutils.widgets.get("tablename")

start = dbutils.widgets.get("dt_start") # now
stop = dbutils.widgets.get("dt_stop") # now

if start == datetime.datetime.now().strftime('%Y-%m-%d'):
    start = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


# COMMAND ----------

ingestor = ingestors.IngestorCubo(spark=spark,
                                  catalog=catalog,
                                  schemaname=schemaname,
                                  tablename=tablename)

ingestor.backfill(start, stop)
