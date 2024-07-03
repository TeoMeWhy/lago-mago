import json
from pyspark.sql import types

def table_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND tableName = '{table}'")
                .count())
    return count == 1


def import_schema(tablename):
    with open(f"{tablename}.json", "r") as open_file:
        schema_json = json.load(open_file) ## dicionário

    schema_df = types.StructType.fromJson(schema_json)
    return schema_df