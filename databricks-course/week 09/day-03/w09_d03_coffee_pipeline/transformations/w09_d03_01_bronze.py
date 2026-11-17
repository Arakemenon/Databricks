from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

LANDING = "/Volumes/workspace/coffee/landing/pipeline_orders"

@dp.table(comment="Online orders as they arrived, one row per order")
def bronze_online_orders():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "customer_id BIGINT")
        .load(LANDING)
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingested_at", current_timestamp()))