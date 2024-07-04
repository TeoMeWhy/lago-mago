import delta
import utils

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