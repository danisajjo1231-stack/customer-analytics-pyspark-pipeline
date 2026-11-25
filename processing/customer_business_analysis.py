from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    countDistinct,
    round as spark_round
)

from customer_data_cleaning import (
    create_spark_session,
    clean_customer_data
)


def main():

    spark = create_spark_session()

    try:
        # Step 1: Bronze data ko clean karo
        df_silver = clean_customer_data(spark)

        print("\n===== BUSINESS ANALYTICS =====")

        # Step 2: Sirf known customers ko select karo
        known_customers = df_silver.filter(
            col("Customer ID").isNotNull()
        )

        # Step 3: Transaction value calculate karo
        # Positive quantity aur positive price wali rows
        # ko sales calculation mein include karenge.
        sales_rows = known_customers.filter(
            (col("Quantity") > 0) &
            (col("Price") > 0) &
            (~col("Invoice").startswith("C"))
        )

        sales_summary = (
            sales_rows
            .groupBy("Customer ID")
            .agg(
                spark_round(
                    spark_sum(col("Quantity") * col("Price")),
                    2
                ).alias("gross_sales"),
                countDistinct("Invoice").alias("sales_invoice_count"),
                spark_sum("Quantity").alias("units_sold")
            )
        )

        print("\n===== TOP 10 CUSTOMERS BY GROSS SALES =====")

        sales_summary.orderBy(
            col("gross_sales").desc()
        ).show(10, truncate=False)

        # Step 4: Cancellation aur return records alag analyze karo
        cancellation_rows = known_customers.filter(
            col("Invoice").startswith("C")
        )

        return_rows = known_customers.filter(
            col("Quantity") < 0
        )

        print("\n===== CANCELLATION ANALYTICS =====")

        print(
            "Cancellation rows:",
            cancellation_rows.count()
        )

        print(
            "Distinct cancellation invoices:",
            cancellation_rows.select("Invoice").distinct().count()
        )

        print("\n===== RETURN / NEGATIVE QUANTITY ANALYTICS =====")

        print(
            "Negative quantity rows:",
            return_rows.count()
        )
        cancellation_and_return_rows = known_customers.filter(
            col("Invoice").startswith("C") &
            (col("Quantity") < 0)
        )

        print(
            "Rows that are both cancellations and negative quantity:",
            cancellation_and_return_rows.count()
        )
        # Step 5: Overall business metrics
        print("\n===== OVERALL SALES METRICS =====")

        sales_rows.agg(
            spark_round(
                spark_sum(col("Quantity") * col("Price")),
                2
            ).alias("gross_sales"),
            spark_sum("Quantity").alias("units_sold"),
            countDistinct("Invoice").alias("sales_invoice_count"),
            countDistinct("Customer ID").alias("unique_customers")
        ).show(truncate=False)

    finally:
        spark.stop()
        


if __name__ == "__main__":
    main()

