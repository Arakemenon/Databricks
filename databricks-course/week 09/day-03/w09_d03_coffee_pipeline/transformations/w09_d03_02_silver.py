from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, lit, explode, trim, initcap, split, element_at, coalesce,
    to_timestamp, to_date, round as spark_round,
)

@dp.temporary_view(comment="All order lines, online and shop, before the customer rule")
def all_order_lines():
    bronze = spark.read.table("bronze_online_orders")

    online_lines = (bronze
        .dropDuplicates(["order_id"])
        .withColumn("item", explode(col("items")))
        .withColumn("order_id", col("order_id").cast("int"))
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("city", initcap(trim(col("city"))))
        .withColumn("order_ts", to_timestamp(col("order_ts")))
        .withColumn("order_date", to_date(col("order_ts")))
        .withColumn("channel", coalesce(col("channel"), lit("online")))
        .withColumn("product", col("item.product"))
        .withColumn("qty", col("item.qty").cast("int"))
        .withColumn("price_eur", col("item.price_eur").cast("double"))
        .withColumn("line_amount_eur", spark_round(col("qty") * col("price_eur"), 2))
        .withColumn("source_file_name", element_at(split(col("source_file"), "/"), -1))
        .drop("items", "item", "_rescued_data", "source_file", "ingested_at"))

    shop_lines = (spark.read.table("workspace.coffee.orders")
        .withColumnRenamed("amount_eur", "line_amount_eur")
        .withColumn("order_ts", col("order_date").cast("timestamp"))
        .withColumn("channel", lit("shop"))
        .withColumn("qty", lit(1))
        .withColumn("price_eur", col("line_amount_eur"))
        .withColumn("source_file_name", lit("orders")))

    return online_lines.unionByName(shop_lines)


@dp.materialized_view(comment="One clean row per order line")
def silver_order_lines():
    return spark.read.table("all_order_lines").filter(col("customer_id").isNotNull())


@dp.materialized_view(comment="Order lines without a customer")
def silver_rejected_lines():
    return spark.read.table("all_order_lines").filter(col("customer_id").isNull())