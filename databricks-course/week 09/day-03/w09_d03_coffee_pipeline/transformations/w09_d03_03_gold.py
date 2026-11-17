from pyspark import pipelines as dp
from pyspark.sql.functions import (
    count, count_distinct, approx_count_distinct,
    sum as spark_sum, round as spark_round,
)

@dp.materialized_view(comment="Lines, orders, and revenue per channel")
def gold_revenue_by_channel():
    return (spark.read.table("silver_order_lines")
        .groupBy("channel")
        .agg(count("*").alias("lines"),
             count_distinct("order_id").alias("orders"),
             spark_round(spark_sum("line_amount_eur"), 2).alias("revenue_eur")))


@dp.materialized_view(comment="Quantity, revenue, and customers per product")
def gold_product_revenue():
    products = spark.read.table("workspace.coffee.products")
    return (spark.read.table("silver_order_lines")
        .join(products, on="product", how="left")
        .groupBy("product", "category")
        .agg(spark_sum("qty").alias("qty"),
             spark_round(spark_sum("line_amount_eur"), 2).alias("revenue_eur"),
             approx_count_distinct("customer_id").alias("customers")))