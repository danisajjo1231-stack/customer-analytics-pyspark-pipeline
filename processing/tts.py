from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, when
)

spark = (
    SparkSession.builder
    .appName("SilverValidation")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(
    "datasets/silver/online_retail_cleaned"
)

df.groupBy("transaction_type").agg(
    count("*").alias("TotalRecords"),

    sum(col("is_cancelled_invoice").cast("long"))
        .alias("CancelledInvoices"),

    sum(col("is_return").cast("long"))
        .alias("Returns"),

    sum(col("is_zero_price").cast("long"))
        .alias("ZeroPrice"),

    sum(col("is_negative_price").cast("long"))
        .alias("NegativePrice"),

    sum(col("is_accounting_adjustment").cast("long"))
        .alias("AccountingAdjustments"),

    sum(
        when(col("Customer ID").isNull(), 1)
        .otherwise(0)
    ).alias("MissingCustomerIDs")

).orderBy(col("TotalRecords").desc()).show(truncate=False)

spark.stop()