import delta
import utils
import tqdm

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
    

class IngestorCDF(IngestorCDC):

    def __init__(self, spark, catalog, schemaname, tablename, id_field, idfield_old):
        
        super().__init__(spark=spark,
                         catalog=catalog,
                         schemaname=schemaname,
                         tablename=tablename,
                         data_format='delta',
                         id_field=id_field,
                         timestamp_field='_commit_timestamp')
        
        self.idfield_old = idfield_old
        self.set_query()
        self.checkpoint_location = f"/Volumes/raw/{schemaname}/cdc/{catalog}_{tablename}_checkpoint/"

    def set_schema(self):
        return

    def set_query(self):
        query = utils.import_query(f"{self.tablename}.sql")
        self.from_table = utils.extract_from(query=query)
        self.original_query = query
        self.query = utils.format_query_cdf(query, "{df}")

    def load(self):
        df = (self.spark.readStream
                   .format('delta')
                   .option("readChangeFeed", "true")
                   .table(self.from_table))
        return df
    
    def save(self, df):
        stream = (df.writeStream
                    .option("checkpointLocation", self.checkpoint_location)
                    .foreachBatch(lambda df, batchID: self.upsert(df) )
                    .trigger(availableNow=True))
        return stream.start()
    
    def upsert(self, df):
        df.createOrReplaceGlobalTempView(f"silver_{self.tablename}")

        query_last = f"""
        SELECT *
        FROM global_temp.silver_{self.tablename}
        WHERE _change_type <> 'update_preimage'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.idfield_old} ORDER BY _commit_timestamp DESC) = 1
        """
        df_last = self.spark.sql(query_last)
        df_upsert = self.spark.sql(self.query, df=df_last)

        (self.deltatable
             .alias("s")
             .merge(df_upsert.alias("d"), f"s.{self.id_field} = d.{self.id_field}") 
             .whenMatchedDelete(condition = "d._change_type = 'delete'")
             .whenMatchedUpdateAll(condition = "d._change_type = 'update_postimage'")
             .whenNotMatchedInsertAll(condition = "d._change_type = 'insert' OR d._change_type = 'update_postimage'")
               .execute())

    def execute(self):
        df = self.load()
        return self.save(df)
    
class IngestorCubo:

    def __init__(self, spark, catalog, schemaname, tablename):
        self.spark = spark
        self.catalog = catalog
        self.schemaname = schemaname
        self.tablename = tablename
        self.table = f"{catalog}.{schemaname}.{tablename}"
        self.set_query()

    def set_query(self):
        self.query = utils.import_query(f"{self.tablename}.sql")

    def load(self, **kwargs):
        df = self.spark.sql(self.query.format(**kwargs))
        return df
    
    def save(self, df, dt_ref):
        self.spark.sql(f"DELETE FROM {self.table} WHERE dtRef = '{dt_ref}'")
        
        (df.write
           .mode("append")
           .saveAsTable(self.table))
        
    def backfill(self, dt_start, dt_stop):
        dates = utils.date_range(dt_start, dt_stop)

        if not utils.table_exists(self.spark, self.catalog, self.schemaname, self.tablename):
            df = self.load(dt_ref=dates.pop(0))
            df.write.saveAsTable(self.table)

        for dt in tqdm.tqdm(dates):
            df = self.load(dt_ref=dt)
            self.save(df=df, dt_ref=dt)
