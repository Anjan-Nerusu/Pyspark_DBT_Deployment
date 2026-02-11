from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from pyspark.sql.functions import concat, col, row_number, current_timestamp,desc
class transformationss:
    def dedup(self, df: DataFrame, dedup_cols: List, cdc: str):
        df = df.withColumn("dedupKey", concat(*dedup_cols))
        df = df.withColumn("dedupCounts", row_number().over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        df = df.filter(col('dedupCounts') == 1)
        df = df.drop("dedupKey", "dedupCounts")

        return df
    
    def process_timestamp(self, df):
        df = df.withColumn("process_timestamp", current_timestamp())
        
        return df
    
    def upsert(self, spark, df, key_cols, table, cdc):

        merge_condition = " AND ".join([f"src.{c} = trg.{c}" for c in key_cols])

        dltObj = DeltaTable.forName(spark, table)

        (
            dltObj.alias("trg")
            .merge(
                df.alias("src"),
                merge_condition
            )
            .whenMatchedUpdateAll(condition=f"src.{cdc} >= trg.{cdc}")
            .whenNotMatchedInsertAll()
            .execute()
        )

        return 1