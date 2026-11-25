from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    countDistinct
)

from customer_data_cleaning import (
    create_spark_session,
    clean_customer_data
)


def main():

    spark = create_spark_session()

    try:
        # Step 1: Cleaned DataFrame hasil karo
        df_silver = clean_customer_data(spark)

        print("\n===== CUSTOMER ANALYTICS =====")

        # Step 2: Sirf un records ko use karo
        # jin mein Customer ID available hai
        known_customers = df_silver.filter(
            col("Customer ID").isNotNull()
        )

        # Step 3: Customer-wise metrics calculate karo
        customer_summary = (
            known_customers
            .groupBy("Customer ID")
            .agg(
                spark_sum(
                    col("Quantity") * col("Price")
                ).alias("transaction_value"),

                countDistinct(
                    "Invoice"
                ).alias("invoice_count"),

                spark_sum(
                    "Quantity"
                ).alias("net_quantity")
            )
        )

        # Step 4: Top 10 customers display karo
        print("\n===== TOP 10 CUSTOMERS =====")

        customer_summary.orderBy(
            col("transaction_value").desc()
        ).show(10, truncate=False)

        # Step 5: Unique customers count karo
        print("\n===== TOTAL UNIQUE CUSTOMERS =====")

        unique_customers = (
            known_customers
            .select("Customer ID")
            .distinct()
            .count()
        )

        print("Unique Customers:", unique_customers)

        # Step 6: Total customer summary records
        print("\n===== CUSTOMER SUMMARY COUNT =====")

        print(
            "Customer summary records:",
            customer_summary.count()
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

